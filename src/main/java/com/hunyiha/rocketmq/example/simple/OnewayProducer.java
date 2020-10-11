package com.hunyiha.rocketmq.example.simple;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

/**
 * 发送单向消息
 *
 * 这种方式主要用在不特别关心发送结果的场景，例如日志发送。
 *
 */
public class OnewayProducer {

    public static void main(String[] args) throws Exception{

        // 实例化消息生产者Producer
        DefaultMQProducer producer = new DefaultMQProducer("Hunyiha_onewaymessage_test");

        // 设置NameServer的地址
        producer.setNamesrvAddr("192.168.202.128:9876");

        // 启动Producer实例
        producer.start();

        for (int i = 0; i < 100; i++) {

            // 创建消息，并指定Topic，Tag和消息体
            Message msg = new Message("Hunyiha_topic_onewaymessage_test" /* Topic */,
                    "TagA" /* Tag */,
                    ("Hello RocketMQ " + i).getBytes(RemotingHelper.DEFAULT_CHARSET) /* Message body */
            );

            // 发送单向消息，没有任何返回结果
            producer.sendOneway(msg);

        }

        // 如果不再发送消息，关闭Producer实例。
        producer.shutdown();
    }
}
