package com.nextflow.plugin.parquet

import org.apache.avro.generic.GenericData.Record

interface ReadRecordListener {

    void nextRecord(Record record)

}