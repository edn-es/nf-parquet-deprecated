package com.nextflow.plugin.dsl.impl

import com.nextflow.plugin.dsl.ParquetDSL

class SchemaDSLImpl implements ParquetDSL.SchemaDSLSpec{

    private final String name
    private final List<String> fields = []

    SchemaDSLImpl(String name){
        this.name = name
    }

    String getName() {
        return name
    }

    @Override
    ParquetDSL.SchemaDSLSpec fields(String field) {
        this.fields.clear()
        this.fields.add(field)
        return this
    }

    @Override
    ParquetDSL.SchemaDSLSpec fields(String[] fields) {
        this.fields.clear()
        this.fields.addAll(fields)
        return this
    }

    List<String>getFields(){
        fields
    }
}
