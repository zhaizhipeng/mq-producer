package com.ysdrzp.producer.rocketmq.quick;

import com.ysdrzp.producer.constant.RocketMQConstant;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

public class Producer {

    public static void main(String[] args) throws Exception {
        //Instantiate with a producer group name.
        DefaultMQProducer producer = new DefaultMQProducer("quick_producer_group");
        // Specify name server addresses.
        producer.setNamesrvAddr(RocketMQConstant.NAME_SERVER_SINGLE);

        //Launch the instance.
        producer.start();
        for (int i = 0; i < 15; i++) {
            //Create a message instance, specifying topic, tag and message body.
            Message msg = new Message("quick_topic",
                    "quick_tag",
                    ("Hello RocketMQ " + i).getBytes(RemotingHelper.DEFAULT_CHARSET)
            );
            //Call send message to deliver message to one of brokers.
            SendResult sendResult = producer.send(msg);
            System.out.printf("%s%n", sendResult);
        }
        //Shut down once the producer instance is not longer in use.
        producer.shutdown();
    }

}
