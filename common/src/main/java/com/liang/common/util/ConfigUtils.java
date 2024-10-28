package com.liang.common.util;

import com.liang.common.dto.Config;
import com.liang.common.service.database.holder.DruidHolder;
import com.liang.common.service.database.holder.HbaseConnectionHolder;
import com.liang.common.service.database.holder.JedisPoolHolder;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@UtilityClass
public class ConfigUtils {
    private final static String SUFFIX = ".yml";
    private static volatile Config config;
    private static volatile AtomicInteger threadNum;

    public static Config createConfig() {
        return createConfig(null);
    }

    public static Config createConfig(String file) {
        Config defaultConfig = initDefaultConfig();
        if (!StringUtils.endsWith(file, SUFFIX)) {
            log.warn("no *{} file for customConfig, return defaultConfig", SUFFIX);
            return defaultConfig;
        }
        Config customConfig = initCustomConfig(file);
        if (customConfig == null) {
            return defaultConfig;
        }
        if (defaultConfig == null) {
            return customConfig;
        }
        mergeConfig(defaultConfig, customConfig);
        return customConfig;
    }

    private static Config initDefaultConfig() {
        log.info("try load default.yml from package resource ...");
        try (InputStream inputStream = ConfigUtils.class.getClassLoader().getResourceAsStream("default.yml")) {
            return YamlUtils.parse(inputStream, Config.class);
        } catch (Exception e) {
            log.error("load default.yml failed", e);
            return null;
        }
    }

    private static Config initCustomConfig(String file) {
        log.info("try load {} from cluster ...", file);
        try (InputStream customConfigStream1 = Files.newInputStream(Paths.get(file))) {
            return YamlUtils.parse(customConfigStream1, Config.class);
        } catch (Exception e1) {
            log.info("try load {} from package resource ...", file);
            try (InputStream customConfigStream2 = ConfigUtils.class.getClassLoader().getResourceAsStream(file)) {
                return YamlUtils.parse(customConfigStream2, Config.class);
            } catch (Exception e2) {
                log.error("load {} failed", file, e2);
                return null;
            }
        }
    }

    private static void mergeConfig(Config defaultConfig, Config customConfig) {
        // jdbc
        if (customConfig.getDbConfigs() == null) {
            customConfig.setDbConfigs(new HashMap<>());
        }
        if (defaultConfig.getDbConfigs() != null) {
            defaultConfig.getDbConfigs().forEach((name, DBConfig) ->
                    customConfig.getDbConfigs().putIfAbsent(name, DBConfig)
            );
        }
        // redis
        if (customConfig.getRedisConfigs() == null) {
            customConfig.setRedisConfigs(new HashMap<>());
        }
        if (defaultConfig.getRedisConfigs() != null) {
            defaultConfig.getRedisConfigs().forEach((name, redisConfig) ->
                    customConfig.getRedisConfigs().putIfAbsent(name, redisConfig)
            );
        }
        // hbase
        if (customConfig.getHbaseConfigs() == null) {
            customConfig.setHbaseConfigs(new HashMap<>());
        }
        if (defaultConfig.getHbaseConfigs() != null) {
            defaultConfig.getHbaseConfigs().forEach((name, hbaseConfig) ->
                    customConfig.getHbaseConfigs().putIfAbsent(name, hbaseConfig)
            );
        }
        // doris
        if (customConfig.getDorisConfigs() == null) {
            customConfig.setDorisConfigs(new HashMap<>());
        }
        if (defaultConfig.getDorisConfigs() != null) {
            defaultConfig.getDorisConfigs().forEach((name, dorisConfig) ->
                    customConfig.getDorisConfigs().putIfAbsent(name, dorisConfig)
            );
        }
    }

    public static Config getConfig() {
        if (config == null)
            log.error("getConfig(), 返回 null");
        return ConfigUtils.config;
    }

    public static void setConfig(Config config) {
        if (config == null) {
            log.error("setConfig(), 入参为 null");
            return;
        }
        if (ConfigUtils.config == null) {
            synchronized (ConfigUtils.class) {
                if (ConfigUtils.config == null) {
                    ConfigUtils.config = config;
                    log.info("config加载: {}", ConfigUtils.config);
                }
            }
        }
    }

    public static void unloadAll() {
        if (threadNum == null) {
            synchronized (ConfigUtils.class) {
                if (threadNum == null) {
                    if (config == null || config.getFlinkConfig() == null) {
                        threadNum = new AtomicInteger(1);
                    } else {
                        threadNum = new AtomicInteger(config.getFlinkConfig().getOtherParallel());
                    }
                }
            }
        }
        if (threadNum.decrementAndGet() == 0) {
            new DruidHolder().closeAll();
            new JedisPoolHolder().closeAll();
            new HbaseConnectionHolder().closeAll();
        }
    }
}
