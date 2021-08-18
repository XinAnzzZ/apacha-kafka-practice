package com.yuhangma.apache.kafka.practice.standalone.consumer;

import com.yuhangma.apache.kafka.practice.standalone.properties.BrokerProperties;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * @author Moore.Ma
 * @version 1.0
 * @description 消费者客户端快速入门
 * @date 2021/8/17 10:59
 */
public class ConsumerFastStart {

    public static void main(String[] args) {
        final Properties properties = new Properties();
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("bootstrap.servers", BrokerProperties.BROKERS);
        properties.setProperty("group.id", BrokerProperties.GROUP_ID);

        // 创建一个消费者实例
        final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        // 订阅主题
        consumer.subscribe(Collections.singleton(BrokerProperties.TOPIC));
        // 循环消费消息
        while (Thread.currentThread().isAlive()) {
            // 拉取消息，超时时间 1s
            final ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000L));
            records.forEach(record -> System.out.println(record.value()));
        }
    }
}
