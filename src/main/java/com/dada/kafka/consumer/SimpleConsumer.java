package com.dada.kafka.consumer;


import com.dada.kafka.common.MessageEntity;
import com.google.gson.Gson;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class SimpleConsumer {// 定义一个消费者

    private final Gson gson = new Gson();// Gson 实例

    @KafkaListener(topics = "${kafka.topic.default}", containerFactory = "kafkaListenerContainerFactory")
    public void receive(MessageEntity message) {
        log.info(gson.toJson(message));// 打印消息内容
    }
}