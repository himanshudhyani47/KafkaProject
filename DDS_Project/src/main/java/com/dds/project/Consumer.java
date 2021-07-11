package com.dds.project;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class Consumer {
    public static void main(String[] args) {
        createConsumer();
        //createConsumer();
        //createConsumer();
    }
    public static void createConsumer(){
        final Logger logger = LoggerFactory.getLogger(Consumer.class);
        //Create variable for string
        final String bootstrapServers = "127.0.0.1:9092,127.0.0.1:9093,127.0.0.1:9094";
        final String consumerGroupID = "cg1";
        //Create and populate properties object
        Properties p = new Properties();
        p.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        p.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.setProperty(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupID);
        p.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        //create consumer
        final KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(p);
        //Subscribe to topics
        consumer.subscribe(Arrays.asList("replica3Consumertopic"));
        //Poll and consume records
        while(true){
            ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(1000));
            for(ConsumerRecord record: records){
                logger.info("Received new record: \n"+"Key: "+record.key() + ","+"Value: "+record.value() + ","+"Topic: "+record.topic() + ","+"Partition: "+record.partition() + ","+"offset: "+record.offset() + "\n");
            }
        }
    }
}
