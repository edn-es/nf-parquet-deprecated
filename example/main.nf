/*
download area1.parquet file (2.5GB) from http://s3-eu-west-1.amazonaws.com/pstorage-ucl-2748466690/16218017/area1.parquet

nextflow.config defines 3 schemas: full, onlyX and XAndY

read the area1.parquet file:

nextflow run main.nf --readSchema full

*/

import java.nio.file.Path

include { fromParquetFile } from 'plugin/nf-parquet'

readSchema = params.readSchema ?: 'full'

channel
        .fromParquetFile( Path.of('area1.parquet'), readSchema)
        | count | view