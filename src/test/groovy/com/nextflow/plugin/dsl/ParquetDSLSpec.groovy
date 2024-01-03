package com.nextflow.plugin.dsl

import spock.lang.Specification

class ParquetDSLSpec extends Specification{

    void "catalogs are required"(){
        when:
        def ret = ParquetDSL.parse({

        })

        then:
        thrown(DSLException)
    }

    void "schemas are required"(){
        when:
        def ret = ParquetDSL.parse({
            catalog {
                field("X").type("double").defaultValue(1.1)
            }
        })

        then:
        thrown(DSLException)
    }

    void "one field and one schema"(){
        when:
        def ret = ParquetDSL.parse({
            catalog {
                field("X").type("double").defaultValue(1.1)
            }
            schema("test").fields("X")
        })

        then:
        ret
        ret.schemas.size() == 1
        ret.schemas['test']
        ret.schemas['test']['fields'].size() == 1
        ret.schemas['test']['fields'][0]['name']=='X'
        ret.schemas['test']['fields'][0]['type']==['double']
    }

    void "one field and one schema in a dsl way"(){
        when:
        def ret = ParquetDSL.parse({
            catalog {
                field"X" type"double" defaultValue 1.1
            }
            schema"test" fields"X"
        })

        then:
        ret
        ret.schemas.size() == 1
        ret.schemas['test']
        ret.schemas['test']['fields'].size() == 1
        ret.schemas['test']['fields'][0]['name']=='X'
        ret.schemas['test']['fields'][0]['type']==['double']
    }

    void "two field and one schema with one field"(){
        when:
        def ret = ParquetDSL.parse({
            catalog {
                field"X" type"double" defaultValue 1.1
                field"Y" type"string" defaultValue 1.1
            }
            schema"test" fields"X"
        })

        then:
        ret
        ret.schemas.size() == 1
        ret.schemas['test']
        ret.schemas['test']['fields'].size() == 1
        ret.schemas['test']['fields'][0]['name']=='X'
        ret.schemas['test']['fields'][0]['type']==['double']
    }

    void "two field and one schema with two field"(){
        when:
        def ret = ParquetDSL.parse({
            catalog {
                field"X" type"double" defaultValue 1.1
                field"Y" type"string" defaultValue 1.1
            }
            schema"test" fields"X", "Y"
        })

        then:
        ret
        ret.schemas.size() == 1
        ret.schemas['test']
        ret.schemas['test']['fields'].size() == 2
        ret.schemas['test']['fields'][0]['name']=='X'
        ret.schemas['test']['fields'][0]['type']==['double']
        ret.schemas['test']['fields'][1]['name']=='Y'
        ret.schemas['test']['fields'][1]['type']==['string']
    }

    void "an optional field"(){
        when:
        def ret = ParquetDSL.parse({
            catalog {
                field "X" type"double" defaultValue 1.1 optional true
            }
            schema"test" fields"X"
        })

        then:
        ret
        ret.schemas.size() == 1
        ret.schemas['test']
        ret.schemas['test']['fields'].size() == 1
        ret.schemas['test']['fields'][0]['name']=='X'
        ret.schemas['test']['fields'][0]['type']==['double',"null"]
    }

    void "field not found"(){
        when:
        def ret = ParquetDSL.parse({
            catalog {
                field"X" type"double" defaultValue 1.1
            }
            schema"test" fields"X", "Y"
        })

        then:
        thrown(DSLException)
    }

    void "invalid type field"(){
        when:
        def ret = ParquetDSL.parse({
            catalog {
                field"X" type"mytype" defaultValue 1.1
            }
            schema"test" fields"X"
        })

        then:
        thrown(DSLException)
    }
}
