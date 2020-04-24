package com.ysdrzp.producer.rocketmq.transaction;

import com.alibaba.fastjson.JSON;
import com.ysdrzp.producer.constant.RocketMQConstant;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

/**
 * 事务消息-保证消息发送零丢失
 * @author 翟志鹏
 */
public class TransactionProducer {

    private static final String MESSAGE_COMMIT = "MESSAGE_COMMIT";

    private static final String MESSAGE_ROLLBACK = "MESSAGE_ROLLBACK";

    public static void main(String[] args) throws MQClientException {

        TransactionMQProducer producer = new TransactionMQProducer("mq_producer_group");

        producer.setNamesrvAddr(RocketMQConstant.NAME_SERVER_SINGLE);

        producer.setTransactionListener(new TransactionListener() {

            /**
             * half 消息接受成功，MQ 响应
             * @param message
             * @param o
             * @return
             */
            @Override
            public LocalTransactionState executeLocalTransaction(Message message, Object o) {
                System.out.println("----executeLocalTransaction----");
                // 验证消息一直未commit 和 rollback 的场景
                // int a = 1 / 0;
                try {
                    System.out.println("----执行本地事务----");
                    System.out.println("----消息commit----");
                    return LocalTransactionState.COMMIT_MESSAGE;
                }catch (Exception e){
                    e.printStackTrace();
                    System.out.println("----本地事务执行异常----");
                    System.out.println("----消息rollback----");
                    System.out.println("----发起退款----");
                    return LocalTransactionState.ROLLBACK_MESSAGE;
                }
            }

            /**
             * 消息一直未处理，MQ 回调确认
             * @param messageExt
             * @return
             */
            @Override
            public LocalTransactionState checkLocalTransaction(MessageExt messageExt) {
                System.out.println("----checkLocalTransaction commit or rollback----");
                if (MESSAGE_COMMIT.equals(commitOrRollback(messageExt.getKeys()))){
                    return LocalTransactionState.COMMIT_MESSAGE;
                }else {
                    return LocalTransactionState.ROLLBACK_MESSAGE;
                }
            }

            /**
             * commit or rollback
             * @param orderSeq
             * @return
             */
            private String commitOrRollback(String orderSeq){
                if ("PO20200424150000000".equals(orderSeq)){
                    return MESSAGE_COMMIT;
                }else{
                    return MESSAGE_ROLLBACK;
                }
            }
        });

        producer.start();

        Message message = new Message("order_pay_success_topic", "TestTag", "PO20200424150000000",  "pay_order_success_message1".getBytes());

        try {
            SendResult sendResult = producer.sendMessageInTransaction(message, null);
            System.out.println("sendResult:"+JSON.toJSONString(sendResult));
        }catch (Exception e){
            e.printStackTrace();
            System.out.println("----订单已关闭----");
            System.out.println("----发起退款----");
        }
    }

}
