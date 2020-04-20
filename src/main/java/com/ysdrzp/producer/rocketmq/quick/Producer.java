package com.ysdrzp.producer.rocketmq.quick;

import com.ysdrzp.producer.conts.Contans;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

public class Producer {

    public static void main(String[] args) throws MQClientException, RemotingException, InterruptedException, MQBrokerException {

        DefaultMQProducer producer = new DefaultMQProducer("test_quick_producer");

        producer.setNamesrvAddr(Contans.NAME_SERVER);

        producer.start();

        for (int i = 0; i < 5; i++){
            // 创建消息
            Message message = new Message("test_quick_topic", // 主题
                    "TagA", // 标签
                    "Key" + i, // 用户自定义的key, 唯一的标识
                    ("Hello RocketMQ" + i).getBytes()); // 消息的内容实体

            // 发送消息
            SendResult sendResult = producer.send(message);
            System.out.println(sendResult);
        }
    }
}
