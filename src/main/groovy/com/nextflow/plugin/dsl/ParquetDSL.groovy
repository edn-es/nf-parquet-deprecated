package com.nextflow.plugin.dsl

import com.nextflow.plugin.dsl.impl.ParquetDSLImpl


class ParquetDSL {

    static interface FieldDSLSpec{
        FieldDSLSpec type(String types)
        FieldDSLSpec optional(boolean optional)
        FieldDSLSpec defaultValue(Object value)
    }

    static interface CatalogDSLSpec{
        FieldDSLSpec field(String name)
    }

    static interface SchemaDSLSpec{
        SchemaDSLSpec fields(String name)
        SchemaDSLSpec fields(String[] name)
    }

    static interface ParquetDSLSpec{
        ParquetDSLSpec catalog(@DelegatesTo(value=CatalogDSLSpec, strategy = Closure.DELEGATE_FIRST)Closure closure)
        SchemaDSLSpec schema(String name)
    }

    static ParquetDSL parse( @DelegatesTo(value = ParquetDSLSpec, strategy = Closure.DELEGATE_FIRST)Closure closure){
        ParquetDSLImpl dsl = new ParquetDSLImpl()
        Closure clone = closure.rehydrate(dsl, dsl, closure.thisObject)
        clone()
        ParquetDSL ret = new ParquetDSL(dsl)
        ret
    }

    private Map<String, Map> schemas = [:]

    ParquetDSL(ParquetDSLImpl dsl){
        validate(dsl)
        schemas = dsl.schemas.inject([:]) {map, schemaDSL->
            def fields = schemaDSL.fields.collect {fieldName->
                def field = dsl.catalog.fields.find {it.name == fieldName }
                return field.toMap()
            }
            def definition = [
                    namespace: "org.nextflow.parquet",
                    type     : "record",
                    name     : "$schemaDSL.name",
                    fields   : fields
            ]
            map[schemaDSL.name] = definition
            map
        } as Map<String, Map>
    }

    void validate(ParquetDSLImpl dsl){
        if( !dsl.catalog ) {
            throw new DSLException("Catalog fields are required")
        }
        if( dsl.schemas.empty ) {
            throw new DSLException("At least one schema is required")
        }
        dsl.schemas.each { schemaDSL ->
            def fields = schemaDSL.fields.collect { fieldName ->
                def field = dsl.catalog.fields.find { it.name == fieldName }
                if (!field) {
                    throw new DSLException("Field $fieldName, referenced in schema $schemaDSL.name, not found")
                }
                if( !['null','boolean','int','long','float','double','bytes','string'].contains(field.type)){
                    throw new DSLException("Field $fieldName, unknow $field.type type")
                }
            }
        }
    }

    Map<String, Map> getSchemas(){
        schemas.clone() as Map<String, Map>
    }
}
