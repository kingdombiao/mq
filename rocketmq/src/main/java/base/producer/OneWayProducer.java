package base.producer;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;

/**
 * 描述:
 * 发送单向消息
 *
 * @author biao
 * @create 2020-04-26 13:50
 */
public class OneWayProducer {
    public static void main(String[] args) throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("SyncProducerGroup");
        producer.setNamesrvAddr("127.0.0.1:9876");
        producer.start();

        for (int i = 1; i <= 10; i++) {
            //创建消息对象，指定topic ,tag以及消息体
            Message message = new Message("rocketMqProducerTopic", "tagA", ("hello world: " + i).getBytes());
            //发送消息
            producer.sendOneway(message);
        }

        producer.shutdown();
    }
}
