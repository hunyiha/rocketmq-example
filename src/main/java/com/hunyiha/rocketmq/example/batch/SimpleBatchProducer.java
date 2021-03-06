package com.hunyiha.rocketmq.example.batch;

import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;

/**
 * 发送简单的批量消息
 *
 * 批量发送消息能显著提高传递小消息的性能。限制是这些批量消息应该有相同的topic，相同的waitStoreMsgOK，而且不能是延时消息。
 * 此外，这一批消息的总大小不应超过4MB。
 */
public class SimpleBatchProducer {

    public static void main(String[] args) throws Exception {

        // 实例化一个生产者来产生延时消息
        DefaultMQProducer producer = new DefaultMQProducer("BatchProducerGroupName");

        //设置NameServer地址
        producer.setNamesrvAddr("192.168.202.128:9876");

        producer.start();

        //If you just send messages of no more than 1MiB at a time, it is easy to use batch
        //Messages of the same batch should have: same topic, same waitStoreMsgOK and no schedule support
        String topic = "Hunyiha_topic_batchmessage_test";

        List<Message> messages = new ArrayList<>();
        messages.add(new Message(topic, "Tag", "OrderID001", "Hello world 0".getBytes()));
        messages.add(new Message(topic, "Tag", "OrderID002", "Hello world 1".getBytes()));
        messages.add(new Message(topic, "Tag", "OrderID003", "Hello world 2".getBytes()));

        producer.send(messages);
    }
}
