package com.nextflow.plugin

import com.nextflow.plugin.dsl.ParquetDSL
import groovy.transform.PackageScope

@PackageScope
class ParquetConfiguration {

    Map<String, Map> schemas = [:]

    ParquetConfiguration(Map map){
        def config = map ?: Collections.emptyMap()
        schemas = parseSchemas(config)
    }

    Map<String, Map> parseSchemas(Map map){
        def config = map.navigate('schemas') ?: {}
        if( !(config instanceof Closure) ){
            throw new RuntimeException("Bad Schemas configuration values. Needs to be a ParquetDSL closure")
        }
        ParquetDSL dsl = ParquetDSL.parse(config)
        return dsl.schemas
    }

}
