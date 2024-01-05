package com.nextflow.plugin.dsl.impl

import com.nextflow.plugin.dsl.ParquetDSL

class FieldDSLImpl implements ParquetDSL.FieldDSLSpec{

    final String name
    String type
    boolean optional
    Object defaultValue

    FieldDSLImpl(String name){
        this.name = name
    }

    @Override
    ParquetDSL.FieldDSLSpec type(String type) {
        this.type = type
        return this
    }

    @Override
    ParquetDSL.FieldDSLSpec optional(boolean optional) {
        this.optional = optional
        return this
    }

    @Override
    ParquetDSL.FieldDSLSpec defaultValue(Object value) {
        this.defaultValue = value
        return this
    }

    Map<String, Object>toMap(){
        List<String> types = [type]
        if( optional )
            types.add("null")
        def ret =[
                name:name,
                type:types
        ]
        if( defaultValue) {
            ret.default = defaultValue
        }
        return ret
    }
}
