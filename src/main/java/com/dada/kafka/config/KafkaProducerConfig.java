package com.dada.kafka.config;

import java.util.HashMap;
import java.util.Map;

import com.dada.kafka.common.MessageEntity;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

@Configuration
@EnableKafka
public class KafkaProducerConfig {

    @Value("${kafka.producer.servers}")
    private String servers;// kafka地址
    @Value("${kafka.producer.retries}")
    private int retries;// 重试次数
    @Value("${kafka.producer.batch.size}")
    private int batchSize;// 批量大小
    @Value("${kafka.producer.linger}")
    private int linger;// 延迟发送
    @Value("${kafka.producer.buffer.memory}")
    private int bufferMemory;// 缓冲区大小


    public Map<String, Object> producerConfigs() {// 配置参数
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        props.put(ProducerConfig.RETRIES_CONFIG, retries);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, batchSize);
        props.put(ProducerConfig.LINGER_MS_CONFIG, linger);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, bufferMemory);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return props;
    }

    /**
     *  创建kafkaTemplate需要使用的producerFactory bean
     * @return
     */
    public ProducerFactory<String, MessageEntity> producerFactory() {
        return new DefaultKafkaProducerFactory<>(producerConfigs(),
                new StringSerializer(),
                new JsonSerializer<MessageEntity>());
    }

    /**
     *  创建kafkaTemplate bean
     * @return
     */
    @Bean
    public KafkaTemplate<String, MessageEntity> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }
}
