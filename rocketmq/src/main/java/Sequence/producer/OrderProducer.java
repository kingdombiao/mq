package Sequence.producer;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * 描述:
 * 发送顺序消息
 *
 * @author biao
 * @create 2020-04-26 14:06
 */
public class OrderProducer {

    public static void main(String[] args) throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("OrderProducerGroup");
        producer.setNamesrvAddr("127.0.0.1:9876");
        producer.start();

        //构建订单消息
        List<OrderStep> orderSteps = OrderStep.buildOrders();

        for (int i = 0; i < orderSteps.size(); i++) {
            //创建消息对象，指定topic ,tag以及消息体
            Message message = new Message("rocketMqProducerTopic", "tagC", (orderSteps.get(i).toString()).getBytes());
            //发送消息
            SendResult sendResult = producer.send(message, (final List<MessageQueue> mqs, final Message msg, final Object arg) -> {
                long orderId = (long) arg;
                int index =(int) orderId % mqs.size();
                return mqs.get(index);
            }, orderSteps.get(i).getOrderId());

            //打印发送结果
            System.out.println(String.format("SendResult status:%s, queueId:%d, body:%s",
                    sendResult.getSendStatus(),
                    sendResult.getMessageQueue().getQueueId(),
                    orderSteps.get(i)));

            TimeUnit.SECONDS.sleep(2);
        }

        producer.shutdown();
    }
}
