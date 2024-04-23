// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use super::credential::AzureCredential;
use crate::azure::credential::*;
use crate::azure::{AzureCredentialProvider, STORE};
use crate::client::get::GetClient;
use crate::client::header::{get_put_result, HeaderConfig};
use crate::client::list::ListClient;
use crate::client::retry::RetryExt;
use crate::client::GetOptionsExt;
use crate::multipart::PartId;
use crate::path::DELIMITER;
use crate::util::{deserialize_rfc1123, GetRange};
use crate::{
    Attribute, Attributes, ClientOptions, GetOptions, ListResult, ObjectMeta, Path, PutMode,
    PutMultipartOpts, PutOptions, PutPayload, PutResult, Result, RetryConfig, TagSet,
};
use async_trait::async_trait;
use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use bytes::{Buf, Bytes};
use chrono::{DateTime, Utc};
use hyper::http::HeaderName;
use reqwest::{
    header::{HeaderMap, HeaderValue, CONTENT_LENGTH, CONTENT_TYPE, IF_MATCH, IF_NONE_MATCH},
    Client as ReqwestClient, Method, RequestBuilder, Response,
};
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt, Snafu};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use url::Url;

const VERSION_HEADER: &str = "x-ms-version-id";
static MS_CACHE_CONTROL: HeaderName = HeaderName::from_static("x-ms-blob-cache-control");
static MS_CONTENT_TYPE: HeaderName = HeaderName::from_static("x-ms-blob-content-type");

static TAGS_HEADER: HeaderName = HeaderName::from_static("x-ms-tags");

/// A specialized `Error` for object store-related errors
#[derive(Debug, Snafu)]
#[allow(missing_docs)]
pub(crate) enum Error {
    #[snafu(display("Error performing get request {}: {}", path, source))]
    GetRequest {
        source: crate::client::retry::Error,
        path: String,
    },

    #[snafu(display("Error getting get response body {}: {}", path, source))]
    GetResponseBody {
        source: reqwest::Error,
        path: String,
    },

    #[snafu(display("Error performing put request {}: {}", path, source))]
    PutRequest {
        source: crate::client::retry::Error,
        path: String,
    },

    #[snafu(display("Error performing delete request {}: {}", path, source))]
    DeleteRequest {
        source: crate::client::retry::Error,
        path: String,
    },

    #[snafu(display("Error performing bulk delete request: {}", source))]
    BulkDeleteRequest { source: crate::client::retry::Error },

    #[snafu(display("Error receiving bulk delete request body: {}", source))]
    BulkDeleteRequestBody { source: reqwest::Error },

    #[snafu(display(
        "Bulk delete request failed due to invalid input: {} (code: {})",
        reason,
        code
    ))]
    BulkDeleteRequestInvalidInput { code: String, reason: String },

    #[snafu(display("Got invalid bulk delete response: {}", reason))]
    InvalidBulkDeleteResponse { reason: String },

    #[snafu(display(
        "Bulk delete request failed for key {}: {} (code: {})",
        path,
        reason,
        code
    ))]
    DeleteFailed {
        path: String,
        code: String,
        reason: String,
    },

    #[snafu(display("Error performing list request: {}", source))]
    ListRequest { source: crate::client::retry::Error },

    #[snafu(display("Error getting list response body: {}", source))]
    ListResponseBody { source: reqwest::Error },

    #[snafu(display("Got invalid list response: {}", source))]
    InvalidListResponse { source: quick_xml::de::DeError },

    #[snafu(display("Error authorizing request: {}", source))]
    Authorization {
        source: crate::azure::credential::Error,
    },

    #[snafu(display("Unable to extract metadata from headers: {}", source))]
    Metadata {
        source: crate::client::header::Error,
    },

    #[snafu(display("ETag required for conditional update"))]
    MissingETag,

    #[snafu(display("Error requesting user delegation key: {}", source))]
    DelegationKeyRequest { source: crate::client::retry::Error },

    #[snafu(display("Error getting user delegation key response body: {}", source))]
    DelegationKeyResponseBody { source: reqwest::Error },

    #[snafu(display("Got invalid user delegation key response: {}", source))]
    DelegationKeyResponse { source: quick_xml::de::DeError },

    #[snafu(display("Generating SAS keys with SAS tokens auth is not supported"))]
    SASforSASNotSupported,

    #[snafu(display("Generating SAS keys while skipping signatures is not supported"))]
    SASwithSkipSignature,
}

impl From<Error> for crate::Error {
    fn from(err: Error) -> Self {
        match err {
            Error::GetRequest { source, path }
            | Error::DeleteRequest { source, path }
            | Error::PutRequest { source, path } => source.error(STORE, path),
            _ => Self::Generic {
                store: STORE,
                source: Box::new(err),
            },
        }
    }
}

/// Configuration for [AzureClient]
#[derive(Debug)]
pub(crate) struct AzureConfig {
    pub account: String,
    pub container: String,
    pub credentials: AzureCredentialProvider,
    pub retry_config: RetryConfig,
    pub service: Url,
    pub is_emulator: bool,
    pub skip_signature: bool,
    pub disable_tagging: bool,
    pub client_options: ClientOptions,
}

impl AzureConfig {
    pub(crate) fn path_url(&self, path: &Path) -> Url {
        let mut url = self.service.clone();
        {
            let mut path_mut = url.path_segments_mut().unwrap();
            if self.is_emulator {
                path_mut.push(&self.account);
            }
            path_mut.push(&self.container).extend(path.parts());
        }
        url
    }
    async fn get_credential(&self) -> Result<Option<Arc<AzureCredential>>> {
        if self.skip_signature {
            Ok(None)
        } else {
            Some(self.credentials.get_credential().await).transpose()
        }
    }
}

/// A builder for a put request allowing customisation of the headers and query string
struct PutRequest<'a> {
    path: &'a Path,
    config: &'a AzureConfig,
    payload: PutPayload,
    builder: RequestBuilder,
    idempotent: bool,
}

impl<'a> PutRequest<'a> {
    fn header(self, k: &HeaderName, v: &str) -> Self {
        let builder = self.builder.header(k, v);
        Self { builder, ..self }
    }

    fn query<T: Serialize + ?Sized + Sync>(self, query: &T) -> Self {
        let builder = self.builder.query(query);
        Self { builder, ..self }
    }

    fn idempotent(self, idempotent: bool) -> Self {
        Self { idempotent, ..self }
    }

    fn with_tags(mut self, tags: TagSet) -> Self {
        let tags = tags.encoded();
        if !tags.is_empty() && !self.config.disable_tagging {
            self.builder = self.builder.header(&TAGS_HEADER, tags);
        }
        self
    }

    fn with_attributes(self, attributes: Attributes) -> Self {
        let mut builder = self.builder;
        let mut has_content_type = false;
        for (k, v) in &attributes {
            builder = match k {
                Attribute::CacheControl => builder.header(&MS_CACHE_CONTROL, v.as_ref()),
                Attribute::ContentType => {
                    has_content_type = true;
                    builder.header(&MS_CONTENT_TYPE, v.as_ref())
                }
            };
        }

        if !has_content_type {
            if let Some(value) = self.config.client_options.get_content_type(self.path) {
                builder = builder.header(&MS_CONTENT_TYPE, value);
            }
        }
        Self { builder, ..self }
    }

    async fn send(self) -> Result<Response> {
        let credential = self.config.get_credential().await?;
        let response = self
            .builder
            .header(CONTENT_LENGTH, self.payload.content_length())
            .with_azure_authorization(&credential, &self.config.account)
            .retryable(&self.config.retry_config)
            .idempotent(self.idempotent)
            .payload(Some(self.payload))
            .send()
            .await
            .context(PutRequestSnafu {
                path: self.path.as_ref(),
            })?;

        Ok(response)
    }
}

#[inline]
fn extend(dst: &mut Vec<u8>, data: &[u8]) {
    dst.extend_from_slice(data);
}

// Write header names as title case. The header name is assumed to be ASCII.
fn title_case(dst: &mut Vec<u8>, name: &[u8]) {
    dst.reserve(name.len());

    // Ensure first character is uppercased
    let mut prev = b'-';
    for &(mut c) in name {
        if prev == b'-' {
            c.make_ascii_uppercase();
        }
        dst.push(c);
        prev = c;
    }
}

fn write_headers(headers: &HeaderMap, dst: &mut Vec<u8>) {
    for (name, value) in headers {
        if name == "content-id" {
            extend(dst, b"Content-ID");
        } else {
            title_case(dst, name.as_str().as_bytes());
        }
        extend(dst, b": ");
        extend(dst, value.as_bytes());
        extend(dst, b"\r\n");
    }
}

fn serialize_part_request(
    dst: &mut Vec<u8>,
    boundary: &str,
    idx: usize,
    request: reqwest::Request,
    relative_url: String,
) {
    // Encode start marker for part
    extend(dst, b"--");
    extend(dst, boundary.as_bytes());
    extend(dst, b"\r\n");

    // Encode part headers
    let mut part_headers = HeaderMap::new();
    part_headers.insert(CONTENT_TYPE, "application/http".parse().unwrap());
    part_headers.insert("Content-Transfer-Encoding", "binary".parse().unwrap());
    part_headers.insert("Content-ID", HeaderValue::from(idx));
    write_headers(&part_headers, dst);
    extend(dst, b"\r\n");

    // Encode the subrequest request-line
    extend(dst, b"DELETE ");
    extend(dst, format!("/{} ", relative_url).as_bytes());
    extend(dst, b"HTTP/1.1");
    extend(dst, b"\r\n");

    // Encode subrequest headers
    write_headers(request.headers(), dst);
    extend(dst, b"\r\n");
    extend(dst, b"\r\n");
}

fn parse_response_part(
    remaining: &mut &[u8],
    results: &mut [Result<Path>],
    paths: &[Path],
) -> Result<()> {
    let invalid_response = |msg: &str| Error::InvalidBulkDeleteResponse {
        reason: msg.to_string(),
    };

    // Parse part headers and retrieve part id
    let mut headers = [httparse::EMPTY_HEADER; 4];
    let id = match httparse::parse_headers(remaining, &mut headers) {
        Ok(httparse::Status::Complete((pos, headers))) => {
            *remaining = &remaining[pos..];
            headers
                .iter()
                .find(|h| h.name.to_lowercase() == "content-id")
                .and_then(|h| std::str::from_utf8(h.value).ok())
                .and_then(|v| v.parse::<usize>().ok())
        }
        _ => return Err(invalid_response("unable to parse parse headers").into()),
    };

    // Parse part response headers
    let mut headers = [httparse::EMPTY_HEADER; 10];
    let mut part_response = httparse::Response::new(&mut headers);
    let content_length = match part_response.parse(remaining) {
        Ok(httparse::Status::Complete(pos)) => {
            *remaining = &remaining[pos..];
            part_response
                .headers
                .iter()
                .find(|h| h.name.to_lowercase() == "content-length")
                .and_then(|h| std::str::from_utf8(h.value).ok())
                .and_then(|v| v.parse::<usize>().ok())
                .unwrap_or_default()
        }
        _ => return Err(invalid_response("unable to parse response").into()),
    };

    let part_body = &remaining[..content_length];

    // Set slice past part body
    *remaining = &remaining[content_length..];

    if !part_body.is_empty() {
        // Skips CRLF after body if non-empty
        *remaining = remaining
            .strip_prefix(b"\r\n")
            .ok_or_else(|| invalid_response("missing part separator"))?;
    }

    match (id, part_response.code) {
        (Some(_id), Some(code)) if (200..300).contains(&code) => {}
        (Some(id), Some(code)) => {
            results[id] = Err(Error::DeleteFailed {
                path: paths[id].as_ref().to_string(),
                code: code.to_string(),
                reason: part_response.reason.unwrap_or_default().to_string(),
            }
            .into());
        }
        (None, Some(code)) => {
            return Err(Error::BulkDeleteRequestInvalidInput {
                code: code.to_string(),
                reason: part_response.reason.unwrap_or_default().to_string(),
            }
            .into())
        }
        _ => return Err(invalid_response("missing part response status code").into()),
    }

    Ok(())
}

#[derive(Debug)]
pub(crate) struct AzureClient {
    config: AzureConfig,
    client: ReqwestClient,
}

impl AzureClient {
    /// create a new instance of [AzureClient]
    pub fn new(config: AzureConfig) -> Result<Self> {
        let client = config.client_options.client()?;
        Ok(Self { config, client })
    }

    /// Returns the config
    pub fn config(&self) -> &AzureConfig {
        &self.config
    }

    async fn get_credential(&self) -> Result<Option<Arc<AzureCredential>>> {
        self.config.get_credential().await
    }

    fn put_request<'a>(&'a self, path: &'a Path, payload: PutPayload) -> PutRequest<'a> {
        let url = self.config.path_url(path);
        let builder = self.client.request(Method::PUT, url);

        PutRequest {
            path,
            builder,
            payload,
            config: &self.config,
            idempotent: false,
        }
    }

    /// Make an Azure PUT request <https://docs.microsoft.com/en-us/rest/api/storageservices/put-blob>
    pub async fn put_blob(
        &self,
        path: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        let builder = self
            .put_request(path, payload)
            .with_attributes(opts.attributes)
            .with_tags(opts.tags);

        let builder = match &opts.mode {
            PutMode::Overwrite => builder.idempotent(true),
            PutMode::Create => builder.header(&IF_NONE_MATCH, "*"),
            PutMode::Update(v) => {
                let etag = v.e_tag.as_ref().context(MissingETagSnafu)?;
                builder.header(&IF_MATCH, etag)
            }
        };

        let response = builder.header(&BLOB_TYPE, "BlockBlob").send().await?;
        Ok(get_put_result(response.headers(), VERSION_HEADER).context(MetadataSnafu)?)
    }

    /// PUT a block <https://learn.microsoft.com/en-us/rest/api/storageservices/put-block>
    pub async fn put_block(
        &self,
        path: &Path,
        part_idx: usize,
        payload: PutPayload,
    ) -> Result<PartId> {
        let content_id = format!("{part_idx:20}");
        let block_id = BASE64_STANDARD.encode(&content_id);

        self.put_request(path, payload)
            .query(&[("comp", "block"), ("blockid", &block_id)])
            .idempotent(true)
            .send()
            .await?;

        Ok(PartId { content_id })
    }

    /// PUT a block list <https://learn.microsoft.com/en-us/rest/api/storageservices/put-block-list>
    pub async fn put_block_list(
        &self,
        path: &Path,
        parts: Vec<PartId>,
        opts: PutMultipartOpts,
    ) -> Result<PutResult> {
        let blocks = parts
            .into_iter()
            .map(|part| BlockId::from(part.content_id))
            .collect();

        let payload = BlockList { blocks }.to_xml().into();
        let response = self
            .put_request(path, payload)
            .with_attributes(opts.attributes)
            .with_tags(opts.tags)
            .query(&[("comp", "blocklist")])
            .idempotent(true)
            .send()
            .await?;

        Ok(get_put_result(response.headers(), VERSION_HEADER).context(MetadataSnafu)?)
    }

    /// Make an Azure Delete request <https://docs.microsoft.com/en-us/rest/api/storageservices/delete-blob>
    pub async fn delete_request<T: Serialize + ?Sized + Sync>(
        &self,
        path: &Path,
        query: &T,
    ) -> Result<()> {
        let credential = self.get_credential().await?;
        let url = self.config.path_url(path);

        self.client
            .request(Method::DELETE, url)
            .query(query)
            .header(&DELETE_SNAPSHOTS, "include")
            .with_azure_authorization(&credential, &self.config.account)
            .send_retry(&self.config.retry_config)
            .await
            .context(DeleteRequestSnafu {
                path: path.as_ref(),
            })?;

        Ok(())
    }

    pub async fn bulk_delete_request(&self, paths: Vec<Path>) -> Result<Vec<Result<Path>>> {
        if paths.is_empty() {
            return Ok(Vec::new());
        }

        let credential = self.get_credential().await?;

        let boundary = format!("batch_{}", uuid::Uuid::new_v4());

        let mut body_bytes = Vec::with_capacity(paths.len() * 256);

        for (idx, path) in paths.iter().enumerate() {
            let url = self.config.path_url(path);

            // Build subrequest with proper authorization
            let request = self
                .client
                .request(Method::DELETE, url)
                .header(CONTENT_LENGTH, HeaderValue::from(0))
                .with_azure_authorization(&credential, &self.config.account)
                .build()
                .unwrap();

            // Url for part requests must be relative and without base
            let relative_url = self.config.service.make_relative(request.url()).unwrap();

            serialize_part_request(&mut body_bytes, &boundary, idx, request, relative_url)
        }

        // Encode end marker
        extend(&mut body_bytes, b"--");
        extend(&mut body_bytes, boundary.as_bytes());
        extend(&mut body_bytes, b"--");
        extend(&mut body_bytes, b"\r\n");

        // Send multipart request
        let url = self.config.path_url(&Path::from("/"));
        let batch_response = self
            .client
            .request(Method::POST, url)
            .query(&[("restype", "container"), ("comp", "batch")])
            .header(
                CONTENT_TYPE,
                HeaderValue::from_str(format!("multipart/mixed; boundary={}", boundary).as_str())
                    .unwrap(),
            )
            .header(CONTENT_LENGTH, HeaderValue::from(body_bytes.len()))
            .body(body_bytes)
            .with_azure_authorization(&credential, &self.config.account)
            .send_retry(&self.config.retry_config)
            .await
            .context(BulkDeleteRequestSnafu {})?;

        let invalid_response = |msg: &str| Error::InvalidBulkDeleteResponse {
            reason: msg.to_string(),
        };

        let content_type = batch_response
            .headers()
            .get(CONTENT_TYPE)
            .ok_or_else(|| invalid_response("missing Content-Type"))?;

        let boundary = content_type
            .as_ref()
            .strip_prefix(b"multipart/mixed; boundary=")
            .ok_or_else(|| invalid_response("invalid Content-Type value"))?
            .to_vec();

        let start_marker = [b"--".as_slice(), boundary.as_slice(), b"\r\n"].concat();

        let end_marker = [b"--".as_slice(), boundary.as_slice(), b"--"].concat();

        let response_body = batch_response
            .bytes()
            .await
            .context(BulkDeleteRequestBodySnafu {})?;

        let mut results: Vec<Result<Path>> = paths.iter().cloned().map(Ok).collect();

        let mut remaining = response_body.as_ref();

        loop {
            // Check for part marker
            remaining = remaining
                .strip_prefix(start_marker.as_slice())
                .ok_or_else(|| invalid_response("missing start marker for part"))?;

            parse_response_part(&mut remaining, &mut results, &paths)?;

            // Workaround for Azurite bug where it does not set content-length but still sends some
            // body. This code skips to the next part or the end.
            if let Some(pos) = remaining
                .windows(start_marker.len())
                .position(|s| s == start_marker.as_slice() || s == end_marker.as_slice())
            {
                remaining = &remaining[pos..];
            }

            if remaining == end_marker {
                break;
            }
        }

        Ok(results)
    }

    /// Make an Azure Copy request <https://docs.microsoft.com/en-us/rest/api/storageservices/copy-blob>
    pub async fn copy_request(&self, from: &Path, to: &Path, overwrite: bool) -> Result<()> {
        let credential = self.get_credential().await?;
        let url = self.config.path_url(to);
        let mut source = self.config.path_url(from);

        // If using SAS authorization must include the headers in the URL
        // <https://docs.microsoft.com/en-us/rest/api/storageservices/copy-blob#request-headers>
        if let Some(AzureCredential::SASToken(pairs)) = credential.as_deref() {
            source.query_pairs_mut().extend_pairs(pairs);
        }

        let mut builder = self
            .client
            .request(Method::PUT, url)
            .header(&COPY_SOURCE, source.to_string())
            .header(CONTENT_LENGTH, HeaderValue::from_static("0"));

        if !overwrite {
            builder = builder.header(IF_NONE_MATCH, "*");
        }

        builder
            .with_azure_authorization(&credential, &self.config.account)
            .retryable(&self.config.retry_config)
            .idempotent(overwrite)
            .send()
            .await
            .map_err(|err| err.error(STORE, from.to_string()))?;

        Ok(())
    }

    /// Make a Get User Delegation Key request
    /// <https://docs.microsoft.com/en-us/rest/api/storageservices/get-user-delegation-key>
    async fn get_user_delegation_key(
        &self,
        start: &DateTime<Utc>,
        end: &DateTime<Utc>,
    ) -> Result<UserDelegationKey> {
        let credential = self.get_credential().await?;
        let url = self.config.service.clone();

        let start = start.to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
        let expiry = end.to_rfc3339_opts(chrono::SecondsFormat::Secs, true);

        let mut body = String::new();
        body.push_str("<?xml version=\"1.0\" encoding=\"utf-8\"?>\n<KeyInfo>\n");
        body.push_str(&format!(
            "\t<Start>{start}</Start>\n\t<Expiry>{expiry}</Expiry>\n"
        ));
        body.push_str("</KeyInfo>");

        let response = self
            .client
            .request(Method::POST, url)
            .body(body)
            .query(&[("restype", "service"), ("comp", "userdelegationkey")])
            .with_azure_authorization(&credential, &self.config.account)
            .retryable(&self.config.retry_config)
            .idempotent(true)
            .send()
            .await
            .context(DelegationKeyRequestSnafu)?
            .bytes()
            .await
            .context(DelegationKeyResponseBodySnafu)?;

        let response: UserDelegationKey =
            quick_xml::de::from_reader(response.reader()).context(DelegationKeyResponseSnafu)?;

        Ok(response)
    }

    /// Creat an AzureSigner for generating SAS tokens (pre-signed urls).
    ///
    /// Depending on the type of credential, this will either use the account key or a user delegation key.
    /// Since delegation keys are acquired ad-hoc, the signer aloows for signing multiple urls with the same key.
    pub async fn signer(&self, expires_in: Duration) -> Result<AzureSigner> {
        let credential = self.get_credential().await?;
        let signed_start = chrono::Utc::now();
        let signed_expiry = signed_start + expires_in;
        match credential.as_deref() {
            Some(AzureCredential::BearerToken(_)) => {
                let key = self
                    .get_user_delegation_key(&signed_start, &signed_expiry)
                    .await?;
                let signing_key = AzureAccessKey::try_new(&key.value)?;
                Ok(AzureSigner::new(
                    signing_key,
                    self.config.account.clone(),
                    signed_start,
                    signed_expiry,
                    Some(key),
                ))
            }
            Some(AzureCredential::AccessKey(key)) => Ok(AzureSigner::new(
                key.to_owned(),
                self.config.account.clone(),
                signed_start,
                signed_expiry,
                None,
            )),
            None => Err(Error::SASwithSkipSignature.into()),
            _ => Err(Error::SASforSASNotSupported.into()),
        }
    }

    #[cfg(test)]
    pub async fn get_blob_tagging(&self, path: &Path) -> Result<Response> {
        let credential = self.get_credential().await?;
        let url = self.config.path_url(path);
        let response = self
            .client
            .request(Method::GET, url)
            .query(&[("comp", "tags")])
            .with_azure_authorization(&credential, &self.config.account)
            .send_retry(&self.config.retry_config)
            .await
            .context(GetRequestSnafu {
                path: path.as_ref(),
            })?;
        Ok(response)
    }
}

#[async_trait]
impl GetClient for AzureClient {
    const STORE: &'static str = STORE;

    const HEADER_CONFIG: HeaderConfig = HeaderConfig {
        etag_required: true,
        last_modified_required: true,
        version_header: Some(VERSION_HEADER),
    };

    /// Make an Azure GET request
    /// <https://docs.microsoft.com/en-us/rest/api/storageservices/get-blob>
    /// <https://docs.microsoft.com/en-us/rest/api/storageservices/get-blob-properties>
    async fn get_request(&self, path: &Path, options: GetOptions) -> Result<Response> {
        // As of 2024-01-02, Azure does not support suffix requests,
        // so we should fail fast here rather than sending one
        if let Some(GetRange::Suffix(_)) = options.range.as_ref() {
            return Err(crate::Error::NotSupported {
                source: "Azure does not support suffix range requests".into(),
            });
        }

        let credential = self.get_credential().await?;
        let url = self.config.path_url(path);
        let method = match options.head {
            true => Method::HEAD,
            false => Method::GET,
        };

        let mut builder = self
            .client
            .request(method, url)
            .header(CONTENT_LENGTH, HeaderValue::from_static("0"))
            .body(Bytes::new());

        if let Some(v) = &options.version {
            builder = builder.query(&[("versionid", v)])
        }

        let response = builder
            .with_get_options(options)
            .with_azure_authorization(&credential, &self.config.account)
            .send_retry(&self.config.retry_config)
            .await
            .context(GetRequestSnafu {
                path: path.as_ref(),
            })?;

        match response.headers().get("x-ms-resource-type") {
            Some(resource) if resource.as_ref() != b"file" => Err(crate::Error::NotFound {
                path: path.to_string(),
                source: format!(
                    "Not a file, got x-ms-resource-type: {}",
                    String::from_utf8_lossy(resource.as_ref())
                )
                .into(),
            }),
            _ => Ok(response),
        }
    }
}

#[async_trait]
impl ListClient for AzureClient {
    /// Make an Azure List request <https://docs.microsoft.com/en-us/rest/api/storageservices/list-blobs>
    async fn list_request(
        &self,
        prefix: Option<&str>,
        delimiter: bool,
        token: Option<&str>,
        offset: Option<&str>,
    ) -> Result<(ListResult, Option<String>)> {
        assert!(offset.is_none()); // Not yet supported

        let credential = self.get_credential().await?;
        let url = self.config.path_url(&Path::default());

        let mut query = Vec::with_capacity(5);
        query.push(("restype", "container"));
        query.push(("comp", "list"));

        if let Some(prefix) = prefix {
            query.push(("prefix", prefix))
        }

        if delimiter {
            query.push(("delimiter", DELIMITER))
        }

        if let Some(token) = token {
            query.push(("marker", token))
        }

        let response = self
            .client
            .request(Method::GET, url)
            .query(&query)
            .with_azure_authorization(&credential, &self.config.account)
            .send_retry(&self.config.retry_config)
            .await
            .context(ListRequestSnafu)?
            .bytes()
            .await
            .context(ListResponseBodySnafu)?;

        let mut response: ListResultInternal =
            quick_xml::de::from_reader(response.reader()).context(InvalidListResponseSnafu)?;
        let token = response.next_marker.take();

        Ok((to_list_result(response, prefix)?, token))
    }
}

/// Raw / internal response from list requests
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct ListResultInternal {
    pub prefix: Option<String>,
    pub max_results: Option<u32>,
    pub delimiter: Option<String>,
    pub next_marker: Option<String>,
    pub blobs: Blobs,
}

fn to_list_result(value: ListResultInternal, prefix: Option<&str>) -> Result<ListResult> {
    let prefix = prefix.unwrap_or_default();
    let common_prefixes = value
        .blobs
        .blob_prefix
        .into_iter()
        .map(|x| Ok(Path::parse(x.name)?))
        .collect::<Result<_>>()?;

    let objects = value
        .blobs
        .blobs
        .into_iter()
        // Note: Filters out directories from list results when hierarchical namespaces are
        // enabled. When we want directories, its always via the BlobPrefix mechanics,
        // and during lists we state that prefixes are evaluated on path segment basis.
        .filter(|blob| {
            !matches!(blob.properties.resource_type.as_ref(), Some(typ) if typ == "directory")
                && blob.name.len() > prefix.len()
        })
        .map(ObjectMeta::try_from)
        .collect::<Result<_>>()?;

    Ok(ListResult {
        common_prefixes,
        objects,
    })
}

/// Collection of blobs and potentially shared prefixes returned from list requests.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct Blobs {
    #[serde(default)]
    pub blob_prefix: Vec<BlobPrefix>,
    #[serde(rename = "Blob", default)]
    pub blobs: Vec<Blob>,
}

/// Common prefix in list blobs response
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct BlobPrefix {
    pub name: String,
}

/// Details for a specific blob
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct Blob {
    pub name: String,
    pub version_id: Option<String>,
    pub is_current_version: Option<bool>,
    pub deleted: Option<bool>,
    pub properties: BlobProperties,
    pub metadata: Option<HashMap<String, String>>,
}

impl TryFrom<Blob> for ObjectMeta {
    type Error = crate::Error;

    fn try_from(value: Blob) -> Result<Self> {
        Ok(Self {
            location: Path::parse(value.name)?,
            last_modified: value.properties.last_modified,
            size: value.properties.content_length as usize,
            e_tag: value.properties.e_tag,
            version: None, // For consistency with S3 and GCP which don't include this
        })
    }
}

/// Properties associated with individual blobs. The actual list
/// of returned properties is much more exhaustive, but we limit
/// the parsed fields to the ones relevant in this crate.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct BlobProperties {
    #[serde(deserialize_with = "deserialize_rfc1123", rename = "Last-Modified")]
    pub last_modified: DateTime<Utc>,
    #[serde(rename = "Content-Length")]
    pub content_length: u64,
    #[serde(rename = "Content-Type")]
    pub content_type: String,
    #[serde(rename = "Content-Encoding")]
    pub content_encoding: Option<String>,
    #[serde(rename = "Content-Language")]
    pub content_language: Option<String>,
    #[serde(rename = "Etag")]
    pub e_tag: Option<String>,
    #[serde(rename = "ResourceType")]
    pub resource_type: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BlockId(Bytes);

impl BlockId {
    pub fn new(block_id: impl Into<Bytes>) -> Self {
        Self(block_id.into())
    }
}

impl<B> From<B> for BlockId
where
    B: Into<Bytes>,
{
    fn from(v: B) -> Self {
        Self::new(v)
    }
}

impl AsRef<[u8]> for BlockId {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

#[derive(Default, Debug, Clone, PartialEq, Eq)]
pub(crate) struct BlockList {
    pub blocks: Vec<BlockId>,
}

impl BlockList {
    pub fn to_xml(&self) -> String {
        let mut s = String::new();
        s.push_str("<?xml version=\"1.0\" encoding=\"utf-8\"?>\n<BlockList>\n");
        for block_id in &self.blocks {
            let node = format!(
                "\t<Uncommitted>{}</Uncommitted>\n",
                BASE64_STANDARD.encode(block_id)
            );
            s.push_str(&node);
        }

        s.push_str("</BlockList>");
        s
    }
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub(crate) struct UserDelegationKey {
    pub signed_oid: String,
    pub signed_tid: String,
    pub signed_start: String,
    pub signed_expiry: String,
    pub signed_service: String,
    pub signed_version: String,
    pub value: String,
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;

    #[test]
    fn deserde_azure() {
        const S: &str = "<?xml version=\"1.0\" encoding=\"utf-8\"?>
<EnumerationResults ServiceEndpoint=\"https://azureskdforrust.blob.core.windows.net/\" ContainerName=\"osa2\">
    <Blobs>
        <Blob>
            <Name>blob0.txt</Name>
            <Properties>
                <Creation-Time>Thu, 01 Jul 2021 10:44:59 GMT</Creation-Time>
                <Last-Modified>Thu, 01 Jul 2021 10:44:59 GMT</Last-Modified>
                <Expiry-Time>Thu, 07 Jul 2022 14:38:48 GMT</Expiry-Time>
                <Etag>0x8D93C7D4629C227</Etag>
                <Content-Length>8</Content-Length>
                <Content-Type>text/plain</Content-Type>
                <Content-Encoding />
                <Content-Language />
                <Content-CRC64 />
                <Content-MD5>rvr3UC1SmUw7AZV2NqPN0g==</Content-MD5>
                <Cache-Control />
                <Content-Disposition />
                <BlobType>BlockBlob</BlobType>
                <AccessTier>Hot</AccessTier>
                <AccessTierInferred>true</AccessTierInferred>
                <LeaseStatus>unlocked</LeaseStatus>
                <LeaseState>available</LeaseState>
                <ServerEncrypted>true</ServerEncrypted>
            </Properties>
            <Metadata><userkey>uservalue</userkey></Metadata>
            <OrMetadata />
        </Blob>
        <Blob>
            <Name>blob1.txt</Name>
            <Properties>
                <Creation-Time>Thu, 01 Jul 2021 10:44:59 GMT</Creation-Time>
                <Last-Modified>Thu, 01 Jul 2021 10:44:59 GMT</Last-Modified>
                <Etag>0x8D93C7D463004D6</Etag>
                <Content-Length>8</Content-Length>
                <Content-Type>text/plain</Content-Type>
                <Content-Encoding />
                <Content-Language />
                <Content-CRC64 />
                <Content-MD5>rvr3UC1SmUw7AZV2NqPN0g==</Content-MD5>
                <Cache-Control />
                <Content-Disposition />
                <BlobType>BlockBlob</BlobType>
                <AccessTier>Hot</AccessTier>
                <AccessTierInferred>true</AccessTierInferred>
                <LeaseStatus>unlocked</LeaseStatus>
                <LeaseState>available</LeaseState>
                <ServerEncrypted>true</ServerEncrypted>
            </Properties>
            <OrMetadata />
        </Blob>
        <Blob>
            <Name>blob2.txt</Name>
            <Properties>
                <Creation-Time>Thu, 01 Jul 2021 10:44:59 GMT</Creation-Time>
                <Last-Modified>Thu, 01 Jul 2021 10:44:59 GMT</Last-Modified>
                <Etag>0x8D93C7D4636478A</Etag>
                <Content-Length>8</Content-Length>
                <Content-Type>text/plain</Content-Type>
                <Content-Encoding />
                <Content-Language />
                <Content-CRC64 />
                <Content-MD5>rvr3UC1SmUw7AZV2NqPN0g==</Content-MD5>
                <Cache-Control />
                <Content-Disposition />
                <BlobType>BlockBlob</BlobType>
                <AccessTier>Hot</AccessTier>
                <AccessTierInferred>true</AccessTierInferred>
                <LeaseStatus>unlocked</LeaseStatus>
                <LeaseState>available</LeaseState>
                <ServerEncrypted>true</ServerEncrypted>
            </Properties>
            <OrMetadata />
        </Blob>
    </Blobs>
    <NextMarker />
</EnumerationResults>";

        let mut _list_blobs_response_internal: ListResultInternal =
            quick_xml::de::from_str(S).unwrap();
    }

    #[test]
    fn deserde_azurite() {
        const S: &str = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>
<EnumerationResults ServiceEndpoint=\"http://127.0.0.1:10000/devstoreaccount1\" ContainerName=\"osa2\">
    <Prefix/>
    <Marker/>
    <MaxResults>5000</MaxResults>
    <Delimiter/>
    <Blobs>
        <Blob>
            <Name>blob0.txt</Name>
            <Properties>
                <Creation-Time>Thu, 01 Jul 2021 10:45:02 GMT</Creation-Time>
                <Last-Modified>Thu, 01 Jul 2021 10:45:02 GMT</Last-Modified>
                <Etag>0x228281B5D517B20</Etag>
                <Content-Length>8</Content-Length>
                <Content-Type>text/plain</Content-Type>
                <Content-MD5>rvr3UC1SmUw7AZV2NqPN0g==</Content-MD5>
                <BlobType>BlockBlob</BlobType>
                <LeaseStatus>unlocked</LeaseStatus>
                <LeaseState>available</LeaseState>
                <ServerEncrypted>true</ServerEncrypted>
                <AccessTier>Hot</AccessTier>
                <AccessTierInferred>true</AccessTierInferred>
                <AccessTierChangeTime>Thu, 01 Jul 2021 10:45:02 GMT</AccessTierChangeTime>
            </Properties>
        </Blob>
        <Blob>
            <Name>blob1.txt</Name>
            <Properties>
                <Creation-Time>Thu, 01 Jul 2021 10:45:02 GMT</Creation-Time>
                <Last-Modified>Thu, 01 Jul 2021 10:45:02 GMT</Last-Modified>
                <Etag>0x1DD959381A8A860</Etag>
                <Content-Length>8</Content-Length>
                <Content-Type>text/plain</Content-Type>
                <Content-MD5>rvr3UC1SmUw7AZV2NqPN0g==</Content-MD5>
                <BlobType>BlockBlob</BlobType>
                <LeaseStatus>unlocked</LeaseStatus>
                <LeaseState>available</LeaseState>
                <ServerEncrypted>true</ServerEncrypted>
                <AccessTier>Hot</AccessTier>
                <AccessTierInferred>true</AccessTierInferred>
                <AccessTierChangeTime>Thu, 01 Jul 2021 10:45:02 GMT</AccessTierChangeTime>
            </Properties>
        </Blob>
        <Blob>
            <Name>blob2.txt</Name>
            <Properties>
                <Creation-Time>Thu, 01 Jul 2021 10:45:02 GMT</Creation-Time>
                <Last-Modified>Thu, 01 Jul 2021 10:45:02 GMT</Last-Modified>
                <Etag>0x1FBE9C9B0C7B650</Etag>
                <Content-Length>8</Content-Length>
                <Content-Type>text/plain</Content-Type>
                <Content-MD5>rvr3UC1SmUw7AZV2NqPN0g==</Content-MD5>
                <BlobType>BlockBlob</BlobType>
                <LeaseStatus>unlocked</LeaseStatus>
                <LeaseState>available</LeaseState>
                <ServerEncrypted>true</ServerEncrypted>
                <AccessTier>Hot</AccessTier>
                <AccessTierInferred>true</AccessTierInferred>
                <AccessTierChangeTime>Thu, 01 Jul 2021 10:45:02 GMT</AccessTierChangeTime>
            </Properties>
        </Blob>
    </Blobs>
    <NextMarker/>
</EnumerationResults>";

        let _list_blobs_response_internal: ListResultInternal = quick_xml::de::from_str(S).unwrap();
    }

    #[test]
    fn to_xml() {
        const S: &str = "<?xml version=\"1.0\" encoding=\"utf-8\"?>
<BlockList>
\t<Uncommitted>bnVtZXJvMQ==</Uncommitted>
\t<Uncommitted>bnVtZXJvMg==</Uncommitted>
\t<Uncommitted>bnVtZXJvMw==</Uncommitted>
</BlockList>";
        let mut blocks = BlockList { blocks: Vec::new() };
        blocks.blocks.push(Bytes::from_static(b"numero1").into());
        blocks.blocks.push("numero2".into());
        blocks.blocks.push("numero3".into());

        let res: &str = &blocks.to_xml();

        assert_eq!(res, S)
    }

    #[test]
    fn test_delegated_key_response() {
        const S: &str = r#"<?xml version="1.0" encoding="utf-8"?>
<UserDelegationKey>
    <SignedOid>String containing a GUID value</SignedOid>
    <SignedTid>String containing a GUID value</SignedTid>
    <SignedStart>String formatted as ISO date</SignedStart>
    <SignedExpiry>String formatted as ISO date</SignedExpiry>
    <SignedService>b</SignedService>
    <SignedVersion>String specifying REST api version to use to create the user delegation key</SignedVersion>
    <Value>String containing the user delegation key</Value>
</UserDelegationKey>"#;

        let _delegated_key_response_internal: UserDelegationKey =
            quick_xml::de::from_str(S).unwrap();
    }
}
