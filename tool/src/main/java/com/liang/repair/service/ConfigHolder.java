package com.liang.repair.service;

import com.liang.common.dto.Config;
import com.liang.common.util.ConfigUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ConfigHolder {
    protected final static Logger log = LoggerFactory.getLogger(ConfigHolder.class);

    static {
        Config config = ConfigUtils.createConfig("config.yml");
        ConfigUtils.setConfig(config);
    }
}
