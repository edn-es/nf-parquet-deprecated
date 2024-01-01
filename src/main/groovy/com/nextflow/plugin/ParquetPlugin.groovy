package com.nextflow.plugin

import groovy.util.logging.Slf4j
import nextflow.plugin.BasePlugin
import org.pf4j.PluginWrapper


@Slf4j
class ParquetPlugin extends BasePlugin{

    ParquetPlugin(PluginWrapper wrapper) {
        super(wrapper)
        initPlugin()
    }

    private void initPlugin(){
        log.info "${this.class.name} plugin initialized"
    }
}
