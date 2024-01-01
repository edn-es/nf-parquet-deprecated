package com.nextflow.plugin

import com.nextflow.plugin.parquet.StreamReader
import groovy.transform.CompileStatic
import groovyx.gpars.dataflow.DataflowWriteChannel
import nextflow.Channel
import nextflow.plugin.extension.Factory
import nextflow.plugin.extension.Function
import nextflow.plugin.extension.PluginExtensionPoint
import nextflow.Session
import nextflow.extension.CH

import java.nio.file.Path

@CompileStatic
class ParquetExtensions extends PluginExtensionPoint{

    private Session session
    private ParquetConfiguration configuration

    @Override
    protected void init(Session session) {
        this.session = session
        this.configuration = parseConfig(session.config.navigate('parquet') as Map)
    }

    protected ParquetConfiguration parseConfig(Map map){
        new ParquetConfiguration(map)
    }

    @Factory
    DataflowWriteChannel fromParquetFile(Path path, String schema){
        if( !configuration.schemas.containsKey(schema) ){
            throw new RuntimeException("Schema $schema is not present in parquet configuration")
        }
        final channel = CH.create()
        session.addIgniter((action) -> emitRawFile(channel, path, configuration.schemas[schema]))
        return channel
    }

    private void emitRawFile(DataflowWriteChannel channel, Path path, List<Map> fields) {
        def streamReader = new StreamReader(path.toUri().toString(), fields)
        streamReader.iterate {record->
            def values = [:]
            fields.each {
                values[it.name] = record.get(it.name.toString())
            }
            channel.bind(values)
        }
        channel.bind(Channel.STOP)
    }
}
