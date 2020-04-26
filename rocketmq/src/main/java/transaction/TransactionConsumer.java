package transaction;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * 描述:
 * ${DESCRIPTION}
 *
 * @author biao
 * @create 2020-04-26 16:41
 */
public class TransactionConsumer {
    public static void main(String[] args) throws Exception{
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("TransactionConsumerGroup");
        consumer.setNamesrvAddr("127.0.0.1:9876");

        consumer.subscribe("transactionTopic","*");

        //设置广播消费模式,默认负载均衡模式消费
        //consumer.setMessageModel(MessageModel.BROADCASTING);

        consumer.registerMessageListener(new MessageListenerConcurrently() {
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                list.forEach((messageExt)->{
                    System.out.println("consumeThread=" + Thread.currentThread().getName() + "," + new String(messageExt.getBody()));
                });
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        consumer.start();
    }
}
