# Verifying Parquet Files

## Overview

Vast DB is able to import Parquet files from Vast S3 in a highly optimised manner.  The Python SDK methods are:

- [vastdb.Table.import_files](https://vastdb-sdk.readthedocs.io/en/latest/table.html#vastdb.table.Table.import_files)
- [vastdb.table.Table.import_partitioned_files](https://vastdb-sdk.readthedocs.io/en/latest/table.html#vastdb.table.Table.import_partitioned_files)
- [vastdb.util.create_table_from_files](https://vastdb-sdk.readthedocs.io/en/latest/util.html#vastdb.util.create_table_from_files)

The vastdb import functionality requires parquet files to have only [supported datatypes](https://github.com/vast-data/vastdb_sdk/blob/main/docs/types.md).  This page provides an example script to verify columns in a parquet file and print out any offending columns.


## Instructions

First run the following:

```python
import pyarrow.parquet as pq

class ParquetChecker:
    def __init__(self, file_path):
        self.file_path = file_path
        self.schema = pq.read_schema(file_path)  # Load only the schema
        self.allowed_types = [
            'UINT8', 'UINT16', 'UINT32', 'UINT64',
            'INT8', 'INT16', 'INT32', 'INT64',
            'STRING', 'LIST', 'STRUCT', 'MAP',
            'BOOL', 'FLOAT', 'DOUBLE', 'BINARY',
            'DECIMAL128', 'DATE32', 'TIMESTAMP',
            'TIME32', 'TIME64', 'NA'
        ]
    
    def check_column_types(self):
        """Checks and prints out columns that do not match the allowed types."""
        print("Checking column types...\n")
        for i in range(len(self.schema)):  # Use len(self.schema) to get number of fields
            field = self.schema[i]
            column_name = field.name
            column_type = field.type
            type_name = str(column_type).upper()

            if type_name not in self.allowed_types:
                print(f"Column '{column_name}' has a non-matching type: {type_name}")
        print("\nColumn type check complete.")

    def print_parquet_schema(self):
        """Prints the schema of the Parquet file."""
        print("Parquet schema:\n")
        print(self.schema)
```

You can use the class like this:

```python
# Example usage
checker = ParquetChecker('CC-MAIN-20180420081400-20180420101400-00000.gz.parquet')
checker.check_column_types()
```

The output is:

```
Checking column types...

Column 'date' has a non-matching type: TIMESTAMP[MS]

Column type check complete.
```

This tells us that the date column needs to be converted to a supported data type before importing the parquet file.


```python
checker.print_parquet_schema()
```

This prints the whole schema, e.g.

```
Parquet schema:

id: string not null
content_length: uint32 not null
date: timestamp[ms] not null
type: string not null
content_type: string
concurrent_to: string
block_digest: string
payload_digest: string
ip_address: string
refers_to: string
target_uri: string
truncated: string
warc_info_id: string
filename: string
profile: string
identified_payload_type: string
segment_number: uint32
segment_origin_id: string
segment_total_length: uint32
body: binary
```