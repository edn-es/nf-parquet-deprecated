package com.nextflow.plugin.dsl.impl

import com.nextflow.plugin.dsl.ParquetDSL

class ParquetDSLImpl implements ParquetDSL.ParquetDSLSpec{

    CatalogDSLImpl catalog
    List<SchemaDSLImpl> schemas = []

    @Override
    ParquetDSL.ParquetDSLSpec catalog(@DelegatesTo(value= ParquetDSL.CatalogDSLSpec, strategy = Closure.DELEGATE_FIRST) Closure closure) {
        CatalogDSLImpl impl = new CatalogDSLImpl()
        Closure clone = closure.rehydrate(impl, impl, closure.thisObject)
        clone()
        this.catalog = impl
        return this
    }

    @Override
    ParquetDSL.SchemaDSLSpec schema(String name) {
        SchemaDSLImpl impl = new SchemaDSLImpl(name)
        schemas.add impl
        return impl
    }
}
