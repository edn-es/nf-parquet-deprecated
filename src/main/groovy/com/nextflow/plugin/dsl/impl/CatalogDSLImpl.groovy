package com.nextflow.plugin.dsl.impl

import com.nextflow.plugin.dsl.ParquetDSL

class CatalogDSLImpl implements ParquetDSL.CatalogDSLSpec{

    List<FieldDSLImpl> fields = []

    @Override
    ParquetDSL.FieldDSLSpec field(String name) {
        FieldDSLImpl ret = new FieldDSLImpl(name)
        fields.add ret
        return ret
    }
}
