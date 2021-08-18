package com.yuhangma.apache.kafka.practice.standalone.producer;

import com.yuhangma.apache.kafka.practice.standalone.properties.BrokerProperties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @author Moore.Ma
 * @version 1.0
 * @description 生产者快速入门
 * @date 2021/8/17 10:51
 */
public class ProducerFastStart {

    public static void main(String[] args) {
        final Properties properties = new Properties();
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        properties.setProperty("bootstrap.servers", BrokerProperties.BROKERS);
        // 创建生产者客户端
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
            // 创建一条记录
            final ProducerRecord<String, String> record = new ProducerRecord<>(
                    BrokerProperties.TOPIC, "Hello, Kafka!");
            producer.send(record);
        }
    }
}
