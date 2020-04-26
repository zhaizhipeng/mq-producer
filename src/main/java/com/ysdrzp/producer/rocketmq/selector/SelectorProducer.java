package com.ysdrzp.producer.rocketmq.selector;

import com.ysdrzp.producer.constant.RocketMQConstant;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;

import java.util.List;

/**
 * 消息过滤机制
 */
public class SelectorProducer {

    public static void main(String[] args) throws MQClientException {

        DefaultMQProducer producer = new DefaultMQProducer("mq_producer_group");

        producer.setNamesrvAddr(RocketMQConstant.NAME_SERVER_SINGLE);

        producer.start();

        try {
            Long orderId = 10000l;
            Message insertMessageA = new Message("selector_topic", "TagA", String.valueOf(orderId), "insert".getBytes());
            insertMessageA.putUserProperty("orderId", String.valueOf(orderId));

            Message updateMessageA = new Message("selector_topic", "TagA", String.valueOf(orderId), "update".getBytes());
            updateMessageA.putUserProperty("orderId", String.valueOf(orderId));

            producer.send(insertMessageA, new MessageQueueSelector() {
                @Override
                public MessageQueue select(List<MessageQueue> list, Message message, Object args) {
                    System.out.println("args:" + args);
                    Long orderId = (Long) args;
                    int index = (int) (orderId % list.size());
                    return list.get(index);
                }
            }, orderId);

            producer.send(updateMessageA, new MessageQueueSelector() {
                @Override
                public MessageQueue select(List<MessageQueue> list, Message message, Object args) {
                    System.out.println("args:" + args);
                    Long orderId = (Long) args;
                    int index = (int) (orderId % list.size());
                    return list.get(index);
                }
            }, orderId);

            orderId = 10001l;
            Message insertMessageB = new Message("selector_topic", "TagB", String.valueOf(orderId), "insert".getBytes());
            insertMessageB.putUserProperty("orderId", String.valueOf(orderId));

            Message updateMessageB = new Message("selector_topic", "TagB", String.valueOf(orderId), "update".getBytes());
            updateMessageB.putUserProperty("orderId", String.valueOf(orderId));

            producer.send(insertMessageB, new MessageQueueSelector() {
                @Override
                public MessageQueue select(List<MessageQueue> list, Message message, Object args) {
                    System.out.println("args:" + args);
                    Long orderId = (Long) args;
                    int index = (int) (orderId % list.size());
                    return list.get(index);
                }
            }, orderId);

            producer.send(updateMessageB, new MessageQueueSelector() {
                @Override
                public MessageQueue select(List<MessageQueue> list, Message message, Object args) {
                    System.out.println("args:" + args);
                    Long orderId = (Long) args;
                    int index = (int) (orderId % list.size());
                    return list.get(index);
                }
            }, orderId);

            orderId = 10002l;
            Message insertMessageC = new Message("selector_topic", "TagC", String.valueOf(orderId), "insert".getBytes());
            insertMessageC.putUserProperty("orderId", String.valueOf(orderId));

            Message updateMessageC = new Message("selector_topic", "TagC", String.valueOf(orderId), "update".getBytes());
            updateMessageC.putUserProperty("orderId", String.valueOf(orderId));

            producer.send(insertMessageC, new MessageQueueSelector() {
                @Override
                public MessageQueue select(List<MessageQueue> list, Message message, Object args) {
                    System.out.println("args:" + args);
                    Long orderId = (Long) args;
                    int index = (int) (orderId % list.size());
                    return list.get(index);
                }
            }, orderId);

            producer.send(updateMessageC, new MessageQueueSelector() {
                @Override
                public MessageQueue select(List<MessageQueue> list, Message message, Object args) {
                    System.out.println("args:" + args);
                    Long orderId = (Long) args;
                    int index = (int) (orderId % list.size());
                    return list.get(index);
                }
            }, orderId);
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
