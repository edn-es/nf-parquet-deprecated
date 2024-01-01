package com.nextflow.plugin

import groovy.transform.PackageScope

@PackageScope
class ParquetConfiguration {

    Map<String, List<Map>> schemas = [:]

    ParquetConfiguration(Map map){
        def config = map ?: Collections.emptyMap()
        schemas = parseSchemas(config)
    }

    Map<String, List<Map>> parseSchemas(Map map){
        def config = map.navigate('schemas') ?: Collections.emptyMap()
        if( !(config instanceof Map) ){
            throw new RuntimeException("Bad Schemas configuration values. Needs to be a named map ")
        }
        config as Map<String, List<Map>>
    }

}
