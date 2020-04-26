package Sequence.cosumer;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.*;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * 描述:
 * 顺序消费消息
 *
 * @author biao
 * @create 2020-04-26 14:05
 */
public class OrderConsumer {

    public static void main(String[] args) throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("OrderConsumerGroup");
        consumer.setNamesrvAddr("127.0.0.1:9876");

        consumer.subscribe("rocketMqProducerTopic","tagC");

        //设置广播消费模式,默认负载均衡模式消费
        //consumer.setMessageModel(MessageModel.BROADCASTING);

        consumer.registerMessageListener((List<MessageExt> msgs, ConsumeOrderlyContext context)->{
            msgs.forEach(messageExt -> {
                System.out.println("consumeThread=" + Thread.currentThread().getName() + ", queueId=" + messageExt.getQueueId() + ", content:" + new String(messageExt.getBody()));
            });
            return ConsumeOrderlyStatus.SUCCESS;
        });
        consumer.start();
    }

}
