package com.nextflow.plugin.parquet

import groovy.json.JsonOutput
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.LocalFileSystem
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.AvroParquetReader
import org.apache.parquet.avro.AvroReadSupport
import org.apache.parquet.hadoop.ParquetReader
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.parquet.io.InputFile


class StreamReader {

    InputFile inputFile
    Map schema

    StreamReader(String path, Map schema) {
        def config = new Configuration()
        config.classLoader = StreamReader.classLoader
        config.set("fs.file.impl", LocalFileSystem.name)
        def hadoopPath = new Path(path)
        this.inputFile = HadoopInputFile.fromPath(hadoopPath, config)
        this.schema = schema
    }

    int iterate(ReadRecordListener listener) {
        def config = new Configuration()
        if( schema ) {
            AvroReadSupport.setRequestedProjection(config, mapToSchema(schema))
        }
        int count = 0;
        try (ParquetReader<GenericData.Record> reader = AvroParquetReader.<GenericData.Record> builder(inputFile)
                .withConf(config)
                .build()) {
            def record = reader.read();
            while (record != null) {
                listener.nextRecord(record)
                count++
                record = reader.read()
            }
        }
        count
    }

    static protected Schema mapToSchema(Map map) {
        def schema = JsonOutput.toJson(map)
        Schema.Parser parser = new Schema.Parser().setValidate(true);
        return parser.parse(schema);
    }

}
