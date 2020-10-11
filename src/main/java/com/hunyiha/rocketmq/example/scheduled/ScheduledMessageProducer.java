package com.hunyiha.rocketmq.example.scheduled;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;

/**
 *  延迟消息发送
 *
 *  定时消息（延迟队列）是指消息发送到broker后，不会立即被消费，等待特定时间投递给真正的topic。
 *  broker有配置项messageDelayLevel，默认值为“1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h”，18个level。
 *  可以配置自定义messageDelayLevel。注意，messageDelayLevel是broker的属性，
 *  不属于某个topic。发消息时，设置delayLevel等级即可：msg.setDelayLevel(level)。level有以下三种情况：
 *       level == 0，消息为非延迟消息
 *       1<=level<=maxLevel，消息延迟特定时间，例如level==1，延迟1s
 *       level > maxLevel，则level== maxLevel，例如level==20，延迟2h
 *  定时消息会暂存在名为SCHEDULE_TOPIC_XXXX的topic中，并根据delayTimeLevel存入特定的queue，
 *    queueId = delayTimeLevel – 1，即一个queue只存相同延迟的消息，保证具有相同发送延迟的消息能够顺序消费。
 *    broker会调度地消费SCHEDULE_TOPIC_XXXX，将消息写入真实的topic。
 */
public class ScheduledMessageProducer {
    public static void main(String[] args) throws Exception {

        // 实例化一个生产者来产生延时消息
        DefaultMQProducer producer = new DefaultMQProducer("ExampleProducerGroup");

        //设置NameServer地址
        producer.setNamesrvAddr("192.168.202.128:9876");

        // 启动生产者
        producer.start();

        int totalMessagesToSend = 100;

        for (int i = 0; i < totalMessagesToSend; i++) {

            Message message = new Message("Hunyiha_topic_scheduledmessage_test", ("Hello scheduled message " + i).getBytes());
            // 设置延时等级3,这个消息将在10s之后发送(现在只支持固定的几个时间,详看delayTimeLevel)
            message.setDelayTimeLevel(3);
            // 发送消息
            producer.send(message);
        }

        // 关闭生产者
        producer.shutdown();
    }
}
