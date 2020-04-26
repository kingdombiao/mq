package transaction;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.client.producer.TransactionSendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.concurrent.TimeUnit;

/**
 * 描述:
 * 事务消息生产者
 *
 * @author biao
 * @create 2020-04-26 16:27
 */
public class TransactionProducer {
    public static void main(String[] args) throws Exception {
        TransactionMQProducer producer = new TransactionMQProducer("transactionGroup");
        producer.setNamesrvAddr("127.0.0.1:9876");

        //添加事务监听
        producer.setTransactionListener(new TransactionListener() {
            /**
             * 在该方法中执行本地事务
             * @param msg
             * @param arg
             * @return
             */
            @Override
            public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
                if(StringUtils.equals("tagA",msg.getTags())){
                    return LocalTransactionState.COMMIT_MESSAGE;
                }else if(StringUtils.equals("tagB",msg.getTags())){
                    return LocalTransactionState.ROLLBACK_MESSAGE;
                }else if(StringUtils.equals("tagC",msg.getTags())){
                    return LocalTransactionState.UNKNOW;
                }
                return LocalTransactionState.UNKNOW;
            }

            /**
             *MQ进行消息事务状态回查
             * @param msg
             * @return
             */
            @Override
            public LocalTransactionState checkLocalTransaction(MessageExt msg) {
                System.out.println(String.format("消息tag:[%s],消息内容:[%s]",msg.getTags(),new String(msg.getBody())));
                return LocalTransactionState.COMMIT_MESSAGE;
            }
        });

        producer.start();

        String[] tags = {"tagA", "tagB", "tagC"};
        for (int i = 0; i < tags.length; i++) {
            Message message = new Message("transactionTopic", tags[i], ("Hello World" + i).getBytes());
            TransactionSendResult sendResult = producer.sendMessageInTransaction(message, null);
            System.out.println("发送结果:" + sendResult);
            TimeUnit.SECONDS.sleep(2);
        }

        //producer.shutdown();
    }
}
