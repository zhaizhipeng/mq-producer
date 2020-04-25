package com.ysdrzp.producer.rocketmq.order;

import com.alibaba.fastjson.JSON;
import com.ysdrzp.producer.constant.RocketMQConstant;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.*;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;

import java.util.List;

/**
 * 顺序消息
 */
public class OrderProducer {

    public static void main(String[] args) throws MQClientException {

        DefaultMQProducer producer = new DefaultMQProducer("mq_producer_group");

        producer.setNamesrvAddr(RocketMQConstant.NAME_SERVER_SINGLE);

        producer.start();

        Long orderId = 1000l;

        Message insertMessage = new Message("order_topic", "TestTag", String.valueOf(orderId), "insert".getBytes());

        Message updateMessage = new Message("order_topic", "TestTag", String.valueOf(orderId), "update".getBytes());

        try {
            SendResult sendResult = producer.send(insertMessage, new MessageQueueSelector() {
                @Override
                public MessageQueue select(List<MessageQueue> list, Message message, Object args) {
                    System.out.println("args:" + args);
                    Long orderId = (Long) args;
                    int index = (int) (orderId % list.size());
                    return list.get(index);
                }
            }, orderId);
            System.out.println("sendResult:"+ JSON.toJSONString(sendResult));

           sendResult = producer.send(updateMessage, new MessageQueueSelector() {
                @Override
                public MessageQueue select(List<MessageQueue> list, Message message, Object args) {
                    System.out.println("args:" + args);
                    Long orderId = (Long) args;
                    int index = (int) (orderId % list.size());
                    return list.get(index);
                }
            }, orderId);
            System.out.println("sendResult:"+ JSON.toJSONString(sendResult));
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
