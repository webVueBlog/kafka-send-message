package com.dada.kafka.producer;

import com.dada.kafka.common.MessageEntity;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;

@Component
public class SimpleProducer {

    @Autowired
    @Qualifier("kafkaTemplate")
    private KafkaTemplate<String, MessageEntity> kafkaTemplate;// 注入KafkaTemplate

    public void send(String topic, MessageEntity message) {
        kafkaTemplate.send(topic, message);
    }

    public void send(String topic, String key, MessageEntity entity) {
        ProducerRecord<String, MessageEntity> record = new ProducerRecord<>(
                topic,
                key,
                entity);

        long startTime = System.currentTimeMillis();// 记录开始时间

        ListenableFuture<SendResult<String, MessageEntity>> future = kafkaTemplate.send(record);// 发送消息
        future.addCallback(new ProducerCallback(startTime, key, entity));// 添加回调函数
    }

}