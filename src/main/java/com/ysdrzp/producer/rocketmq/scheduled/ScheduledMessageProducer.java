package com.ysdrzp.producer.rocketmq.scheduled;

import com.alibaba.fastjson.JSON;
import com.sun.xml.internal.ws.spi.db.DatabindingException;
import com.ysdrzp.producer.constant.RocketMQConstant;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 延迟消息
 */
public class ScheduledMessageProducer {

    public static void main(String[] args) throws MQClientException {

        DefaultMQProducer producer = new DefaultMQProducer(RocketMQConstant.PRODUCER_GROUP);

        producer.setNamesrvAddr(RocketMQConstant.NAME_SERVER_SINGLE);

        producer.start();

        Message schedualedMessage = new Message("scheduled_message_topic", "TestTag", String.valueOf("6666"), "scheduledMessage".getBytes());

        // 延迟 1 分钟
        schedualedMessage.setDelayTimeLevel(5);

        try {
            System.out.println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(System.currentTimeMillis()));
            SendResult sendResult = producer.send(schedualedMessage);
            System.out.println("sendResult:"+ JSON.toJSONString(sendResult));
        }catch (Exception e){
            e.printStackTrace();
        }
    }

}
