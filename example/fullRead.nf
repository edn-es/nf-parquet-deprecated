/*
download area1.parquet file (2.5GB) from http://s3-eu-west-1.amazonaws.com/pstorage-ucl-2748466690/16218017/area1.parquet

To be used it with noSchema.config

read the area1.parquet file:

nextflow -c noSchema.config run fullRead.nf

*/

import java.nio.file.Path

include { fromParquetFile } from 'plugin/nf-parquet'

channel
        .fromParquetFile( Path.of('area1.parquet') )
        .randomSample( 10 )
        .view()