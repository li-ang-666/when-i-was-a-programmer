package com.liang.repair.impl;

import com.liang.repair.service.ConfigHolder;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaSender extends ConfigHolder {
    private static final String TOPIC = "9349c.proto.test.company_patent_basic_info_index_split";
    private static final String MESSAGE_KEY = "{}";
    private static final String MESSAGE_VALUE = "{}";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.99.202.90:9092,10.99.206.80:9092,10.99.199.2:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, MESSAGE_KEY, MESSAGE_VALUE);
        producer.send(record);
        producer.flush();
        producer.close();
    }
}
