package com.dada.kafka.config;


import com.dada.kafka.common.MessageEntity;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafka
public class KafkaConsumerConfig {

    @Value("${kafka.consumer.servers}")
    private String servers;
    @Value("${kafka.consumer.enable.auto.commit}")
    private boolean enableAutoCommit;//是否自动提交offset
    @Value("${kafka.consumer.session.timeout}")
    private String sessionTimeout;//会话超时时间
    @Value("${kafka.consumer.auto.commit.interval}")
    private String autoCommitInterval;//自动提交offset间隔时间
    @Value("${kafka.consumer.group.id}")
    private String groupId;//消费者组id
    @Value("${kafka.consumer.auto.offset.reset}")
    private String autoOffsetReset;//offset重置策略
    @Value("${kafka.consumer.concurrency}")
    private int concurrency;//消费者并发处理数

    @Bean
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, MessageEntity>> kafkaListenerContainerFactory() {
        // 创建消费者工厂
        ConcurrentKafkaListenerContainerFactory<String, MessageEntity> factory = new ConcurrentKafkaListenerContainerFactory<>();
        // 设置kafka服务器地址
        factory.setConsumerFactory(consumerFactory());// 设置消费者工厂
        factory.setConcurrency(concurrency);// 设置消费者并发处理数
        factory.getContainerProperties().setPollTimeout(1500);// 设置poll方法的超时时间
        return factory;// 返回工厂实例
    }

    private ConsumerFactory<String, MessageEntity> consumerFactory() {// 创建消费者工厂
        return new DefaultKafkaConsumerFactory<>(// 设置消费者属性
                consumerConfigs(),// 设置key的反序列化方式
                new StringDeserializer(),// 设置value的反序列化方式
                // 设置value的反序列化方式
                new JsonDeserializer<>(MessageEntity.class)
        );
    }

    /**
     *  消费者属性
     * @return
     */
    private Map<String, Object> consumerConfigs() {
        Map<String, Object> propsMap = new HashMap<>();
        propsMap.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        propsMap.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, enableAutoCommit);
        propsMap.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, autoCommitInterval);
        propsMap.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, sessionTimeout);
        propsMap.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        propsMap.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        propsMap.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        propsMap.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
        return propsMap;
    }
}