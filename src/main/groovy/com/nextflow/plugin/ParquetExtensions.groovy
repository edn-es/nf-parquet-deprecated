package com.nextflow.plugin

import com.nextflow.plugin.parquet.StreamReader
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import groovyx.gpars.dataflow.DataflowWriteChannel
import nextflow.Channel
import nextflow.plugin.extension.Factory
import nextflow.plugin.extension.PluginExtensionPoint
import nextflow.Session
import nextflow.extension.CH

import java.nio.file.Path

@Slf4j
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
    DataflowWriteChannel fromParquetFile(Path path, String schema=null){
        if( schema && !configuration.schemas.containsKey(schema) ){
            throw new RuntimeException("Schema $schema is not present in parquet configuration")
        }
        final channel = CH.create()
        session.addIgniter((action) -> emitRawFile(channel, path, schema))
        return channel
    }

    private void emitRawFile(DataflowWriteChannel channel, Path path, String schema) {
        try {
            def schemaConfig = schema ? configuration.schemas[schema] : null
            log.info "Start reading $path, with projection $schema"
            def streamReader = new StreamReader(path.toUri().toString(), schemaConfig)
            streamReader.iterate { record ->
                channel.bind(record)
            }
            log.info "Finished emitted $path, with projection $schema"
            channel.bind(Channel.STOP)
        }catch (Throwable t){
            log.error("Error reading $path parquet file", t)
            session.abort(t)
        }
    }
}
