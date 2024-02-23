package com.dada.kafka.producer;


import com.dada.kafka.common.MessageEntity;
import com.google.gson.Gson;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.kafka.support.SendResult;
import org.springframework.lang.Nullable;
import org.springframework.util.concurrent.ListenableFutureCallback;

@Slf4j
public class ProducerCallback implements ListenableFutureCallback<SendResult<String, MessageEntity>> {

    private final long startTime;// 开始时间
    private final String key;// 消息key
    private final MessageEntity message;// 消息内容

    private final Gson gson = new Gson();// 序列化工具

    public ProducerCallback(long startTime, String key, MessageEntity message) {
        this.startTime = startTime;// 记录开始时间
        this.key = key;
        this.message = message;
    }


    @Override
    public void onSuccess(@Nullable SendResult<String, MessageEntity> result) {
        if (result == null) {
            return;
        }
        long elapsedTime = System.currentTimeMillis() - startTime;// 计算耗时

        RecordMetadata metadata = result.getRecordMetadata();// 获取元数据信息
        if (metadata != null) {// 打印消息发送成功后的元数据信息
            StringBuilder record = new StringBuilder();// 构建记录字符串
            record.append("message(")
                    .append("key = ").append(key).append(",")
                    .append("message = ").append(gson.toJson(message)).append(")")
                    .append("sent to partition(").append(metadata.partition()).append(")")
                    .append("with offset(").append(metadata.offset()).append(")")
                    .append("in ").append(elapsedTime).append(" ms");// 拼接记录字符串
            log.info(record.toString());
        }
    }

    @Override
    public void onFailure(Throwable ex) {// 处理异常
        ex.printStackTrace();
    }
}

