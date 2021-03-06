package base.producer;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

import java.util.concurrent.TimeUnit;

/**
 * 描述:
 * 发送异步消息
 *
 * @author biao
 * @create 2020-04-26 13:18
 */
public class AsyncProducer {

    public static void main(String[] args) throws Exception {

        DefaultMQProducer producer = new DefaultMQProducer("SyncProducerGroup");
        producer.setNamesrvAddr("127.0.0.1:9876");
        producer.start();

        for (int i = 1; i <= 10; i++) {
            //创建消息对象，指定topic ,tag以及消息体
            Message message = new Message("rocketMqProducerTopic", "tagB", ("hello world: " + i).getBytes());
            //发送消息
            producer.send(message, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    //打印发送结果
                    System.out.println("发送结果：" + sendResult);
                }

                @Override
                public void onException(Throwable throwable) {
                    //发送失败
                    System.out.println("发送异常：" + throwable);
                }
            });

            TimeUnit.SECONDS.sleep(1);
        }

        producer.shutdown();
    }


}
