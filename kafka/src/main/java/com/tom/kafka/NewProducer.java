package com.tom.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.Scanner;

/**
 * ClassName: NewProducer
 * Description:
 *
 * @author Mi_dad
 * @date 2019/11/21 21:53
 */
public class NewProducer {
    public static void main(String[] args) {
        //配置信息
        Properties props = new Properties();
        //kafka集群
        props.put("bootstrap.servers", "hadoop201:9092");
        //应答策略
        props.put("acks", "all");
        //重试次数
        props.put("retries", 0);
        //缓存大小
        props.put("batch.size", 16384);
        //请求延时
        props.put("linger.ms", 1);
        //发送缓冲区内存大小
        props.put("buffer.memory", 33554432);
        //key序列化
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //value序列化
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        //自定义分区
        props.put("partitioner.class", "com.tom.kafka.MyPartition");

        KafkaProducer<String, String> kp = new KafkaProducer<>(props);
//        for (int i = 0; i < 100; i++) {
//            kp.send(new ProducerRecord<>("second", String.valueOf(i)));
//        }

        /**
         *带回调函数
         */
        for (int i = 0; i < 100; i++) {
            kp.send(new ProducerRecord<String,String>("first", String.valueOf(i)), new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    System.out.println("offset:"+metadata.offset()+"----partition:"+metadata.partition()+"----key:"+metadata.serializedKeySize()+"----value:"+metadata.serializedValueSize());
                }
            });
        }
        kp.close();



    }
}
