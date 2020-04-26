package delay;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

import java.util.concurrent.TimeUnit;

/**
 * 描述:
 * 延迟消息生产者
 *
 * @author biao
 * @create 2020-04-26 16:13
 */
public class DelayProducer {
    public static void main(String[] args) throws Exception {

        DefaultMQProducer producer = new DefaultMQProducer("DelayProducerGroup");
        producer.setNamesrvAddr("127.0.0.1:9876");
        producer.start();

        for(int i=1;i<=10;i++){
            //创建消息对象，指定topic ,tag以及消息体
            Message message = new Message("DelayTopic", "tagD", ("hello world: " + i).getBytes());

            //设置延迟等级  messageDelayLevel = "1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h";
            message.setDelayTimeLevel(2);

            //发送消息
            SendResult sendResult = producer.send(message);

            //打印发送结果
            System.out.println("发送结果："+sendResult);

            //TimeUnit.SECONDS.sleep(2);
        }

        producer.shutdown();
    }
}
