package com.misaya.flink.utils;

import com.alibaba.fastjson.JSON;
import com.misaya.flink.model.Student;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class KafkaUtils2 {


    public static final String broker_list = "localhost:9092";
    public static final String topic = "student";

    public static void writeToKafka() {


        Properties properties = new Properties();

        properties.put("bootstrap.servers", broker_list);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer producer = new KafkaProducer<String, String>(properties);


        for (int i = 1; i <= 100; i++) {
            Student student = new Student(i, "zhisheng" + i, "password" + i, 18 + i);
            ProducerRecord record = new ProducerRecord<String, String>(topic, null, null, JSON.toJSONString(student));
            producer.send(record);
            System.out.println("发送数据：" + JSON.toJSONString(student));

        }

        producer.flush();




    }

    public static void main(String[] args) {
        writeToKafka();

    }

}
