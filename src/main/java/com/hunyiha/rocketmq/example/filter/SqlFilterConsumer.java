/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hunyiha.rocketmq.example.filter;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 *  SQL过滤基本语法:
 *      数值比较，比如：>，>=，<，<=，BETWEEN，=；
 *      字符比较，比如：=，<>，IN；
 *      IS NULL 或者 IS NOT NULL；
 *      逻辑符号 AND，OR，NOT；
 *
 *   常量支持类型为：
 *      数值，比如：123，3.1415；
 *      字符，比如：'abc'，必须用单引号包裹起来；
 *      NULL，特殊的常量
 *      布尔值，TRUE 或 FALSE
 *
 *    Broker一定要开启属性过滤, 即enablePropertyFilter=true
 *   只有使用push模式的消费者并且在broker开启属性过滤(enablePropertyFilter=true)才能用使用SQL92标准的sql语句,
 *   否则抛出异常:The broker does not support consumer to filter message by SQL92
 */
public class SqlFilterConsumer {

    public static void main(String[] args) throws Exception {

        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("please_rename_unique_group_name");

        //设置NameServer地址
        consumer.setNamesrvAddr("192.168.202.128:9876");

        // Broker一定要开启属性过滤, 即enablePropertyFilter=true
        consumer.subscribe("SqlFilterTest",
            MessageSelector.bySql("(TAGS is not null and TAGS in ('TagA', 'TagB'))" +
                "and (a is not null and a between 0 and 3)"));

        consumer.registerMessageListener(new MessageListenerConcurrently() {

            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                                                            ConsumeConcurrentlyContext context) {
                System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), msgs);
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        consumer.start();
        System.out.printf("Consumer Started.%n");
    }
}
