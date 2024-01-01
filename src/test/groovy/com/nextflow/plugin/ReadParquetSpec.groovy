package com.nextflow.plugin

import groovy.json.JsonOutput
import nextflow.Channel
import nextflow.plugin.Plugins
import nextflow.plugin.TestPluginDescriptorFinder
import nextflow.plugin.TestPluginManager
import nextflow.plugin.extension.PluginExtensionProvider
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.parquet.hadoop.ParquetFileWriter
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.hadoop.util.HadoopOutputFile
import org.apache.parquet.io.OutputFile
import org.pf4j.PluginDescriptorFinder
import spock.lang.Shared
import test.Dsl2Spec
import test.MockScriptRunner

import java.nio.file.Files
import java.nio.file.Path
import java.time.Instant

class ReadParquetSpec extends Dsl2Spec{

    @Shared String pluginsMode

    def setup() {
        // reset previous instances
        PluginExtensionProvider.reset()
        // this need to be set *before* the plugin manager class is created
        pluginsMode = System.getProperty('pf4j.mode')
        System.setProperty('pf4j.mode', 'dev')
        // the plugin root should
        def root = Path.of('.').toAbsolutePath().normalize()
        def manager = new TestPluginManager(root){
            @Override
            protected PluginDescriptorFinder createPluginDescriptorFinder() {
                return new TestPluginDescriptorFinder(){
                    @Override
                    protected Path getManifestPath(Path pluginPath) {
                        return pluginPath.resolve('build/tmp/jar/MANIFEST.MF')
                    }
                }
            }
        }
        Plugins.init(root, 'dev', manager)
    }

    def cleanup() {
        Plugins.stop()
        PluginExtensionProvider.reset()
        pluginsMode ? System.setProperty('pf4j.mode',pluginsMode) : System.clearProperty('pf4j.mode')
    }

    def 'schema is required' () {
        given:
        def dir = Files.createTempDirectory("nf")
        def file = Files.createTempFile(dir, "test", ".parquet")

        when:
        def SCRIPT = """
            import java.nio.file.Path
            include {fromParquetFile} from 'plugin/nf-parquet'
            def path = Path.of('${file.toAbsolutePath()}')
            channel.fromParquetFile( path, 'simpleSchema') 
            """
        and:
        new MockScriptRunner([parquet:[]]).setScript(SCRIPT).execute()

        then:
        thrown(RuntimeException)
    }

    def 'should read a parquet file' () {
        given:
            def dir = Files.createTempDirectory("nf")
            def file = Files.createTempFile(dir, "test", ".parquet")
            def path = new org.apache.hadoop.fs.Path(file.toAbsolutePath().toString())
            def schema = parseSchema()
            def outputFile = HadoopOutputFile.fromPath(path, new Configuration())
            try (ParquetWriter<GenericData.Record> writer = AvroParquetWriter.<GenericData.Record> builder(outputFile)
                    .withSchema(schema)
                    .withCompressionCodec(CompressionCodecName.SNAPPY)
                    .withRowGroupSize(ParquetWriter.DEFAULT_BLOCK_SIZE)
                    .withPageSize(ParquetWriter.DEFAULT_PAGE_SIZE)
                    .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                    .withConf(new Configuration())
                    .withValidation(false)
                    .withDictionaryEncoding(false)
                    .build()) {
                def record = new GenericData.Record(schema)
                record.put("myString", "hi world of parquet!".toString())
                record.put("myInteger", 12345)
                record.put("myDateTime", Instant.now().toEpochMilli())
                writer.write(record)
            }

        when:
        def SCRIPT = """
            import java.nio.file.Path
            include {fromParquetFile} from 'plugin/nf-parquet'
            def path = Path.of('${file.toAbsolutePath()}')
            channel.fromParquetFile( path, 'simpleSchema') 
            """
        and:
        def result = new MockScriptRunner([
                parquet:[
                        schemas:[
                                simpleSchema:[
                                        [
                                                name:"myString",
                                                type: ["string","null"]
                                        ]
                                ]
                        ]
                ]
        ]).setScript(SCRIPT).execute()
        then:
        "$result.val.myString".toString() == 'hi world of parquet!'
        result.val == Channel.STOP
    }

    private static Schema parseSchema() {
        def json = [
                namespace: "org.myorganization.mynamespace",
                type:"record",
                name:"myrecordname",
                fields:[
                        [name:"myString", type:["string","null"]],
                        [name:"myInteger", type:"int"],
                        [name:"myDateTime", type:[
                                [
                                        type:"long",
                                        logicalType:"timestamp-millis"
                                ],
                                "null"
                        ]],
                ]
        ]
        def schema = JsonOutput.toJson(json)
        Schema.Parser parser = new Schema.Parser().setValidate(true);
        return parser.parse(schema);
    }
}
