package com.liang.tool.impl;

import com.liang.tool.service.ConfigHolder;

public class KafkaAdmin extends ConfigHolder {
    public static void main(String[] args) throws Exception {
        /*Properties properties = new Properties();
        properties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "10.99.202.90:9092,10.99.199.2:9092,10.99.203.176:9092,10.99.198.184:9092,10.99.206.80:9092,10.99.200.29:9092,10.99.198.170:9092");
        properties.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 3600 * 1000);
        AdminClient adminClient = AdminClient.create(properties);
        String[] split = IOUtils.toString(ConfigHolder.class.getClassLoader().getResourceAsStream("topics")).trim().split("\n");
        for (String s : split) {
            log.info("delete: {}", s);
            try {
                adminClient.deleteTopics(Collections.singleton(s)).all().get();
            } catch (Exception e) {
                log.error("delete  {} error", s, e);
            }
        }*/
    }
}
