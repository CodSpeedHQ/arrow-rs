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

use std::path::Path;
use arrow_array::RecordBatch;
use arrow_cast::pretty::{pretty_format_batches};
use parquet::file::metadata::ParquetMetaData;

/// This example demonstrates advanced usage of Parquet metadata.
///
/// This is designed to show how to store Parquet metadata somewhere other than
/// the Parquet file itself, and how to use that metadata to read the file. This
/// can be used, for example, to store metadata for parquet files on remote
/// object storage (e.g. S3)  in a local file, use a query engine like
/// DataFusion to figure out which files to read, and then read the files with a
/// single object store request.
///
/// Specifically it:
/// 1. It reads the metadata of a Parquet file
/// 2. Removes some column statistics from the metadata (to make them smaller)
/// 3. Stores the metadata in a separate file
/// 4. Reads the metadata from the separate file and uses that to read the Parquet file
///
/// Without this API, to implement the functionality you need to implement
/// a conversion to/from some other structs that can be serialized/deserialized.

#[tokio::main(flavor = "current_thread")]
async fn main() -> parquet::errors::Result<()> {
    let testdata = arrow::util::test_util::parquet_test_data();
    let parquet_path = format!("{testdata}/alltypes_plain.parquet");
    let metadata_path = "thift_metadata.dat"; // todo tempdir for now use local file to inspect it

    let metadata = get_metadata_from_parquet_file(&parquet_path).await;
    let metadata = prepare_metadata(metadata);
    write_metadata_to_file(metadata, &metadata_path);

    // now read the metadata from the file and use it to read the Parquet file
    let metadata = read_metadata_from_file(&metadata_path);
    let batches = read_parquet_file_with_metadata(&parquet_path, metadata);

    // display the results
    let batches_string = pretty_format_batches(&batches).unwrap()
        .to_string();
    let batches_lines :Vec<_> =  batches_string
        .split('\n')
        .collect();

    assert_eq!(batches_lines,
        vec!["todo"]
    );

    Ok(())
}

/// Reads the metadata from a parquet file
async fn get_metadata_from_parquet_file(file: impl AsRef<Path>) -> ParquetMetaData {
    todo!();
}

/// modifies the metadata to reduce its size
fn prepare_metadata(metadata: ParquetMetaData) -> ParquetMetaData {
    todo!();
}

/// writes the metadata to a file
///
/// The data is stored using the same thrift format as the Parquet file metadata
fn write_metadata_to_file(metadata: ParquetMetaData, file: impl AsRef<Path>) {
    todo!();
}

/// Reads the metadata from a file
///
/// This function reads the format written by `write_metadata_to_file`
fn read_metadata_from_file(file: impl AsRef<Path>) -> ParquetMetaData {
    todo!();
}

/// Reads the Parquet file using the metadata
///
/// This shows how to read the Parquet file using previously read metadata
/// instead of the metadata in the Parquet file itself. This avoids an IO /
/// having to fetch and decode the metadata from the Parquet file before
/// beginning to read it.
///
/// In this example, we read the results as Arrow record batches
fn read_parquet_file_with_metadata(file: impl AsRef<Path>, metadata: ParquetMetaData) -> Vec<RecordBatch>{
    todo!();
}


