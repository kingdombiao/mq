package com.kingdombiao.qos;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

import java.io.IOException;

/**
 * 描述:
 * 批量确认消费者
 *
 * @author biao
 * @create 2019-09-04 14:45
 */
public class BatchAckConsumer extends DefaultConsumer {

    private int msgCount=0;

    public BatchAckConsumer(Channel channel) {
        super(channel);
    }

    @Override
    public void handleDelivery(String consumerTag, Envelope envelope,
                               AMQP.BasicProperties properties, byte[] body) throws IOException {

        String msg = new String(body);

        System.out.println("批量消费者Received["+envelope.getRoutingKey()
                +"]"+msg);

        msgCount++;


        if("stop".equals(msg)){
            this.getChannel().basicAck(envelope.getDeliveryTag(),true);
            System.out.println("批量消费者进行最后部分业务消息的确认-------------");
        }else {
            //getChannel().basicAck(envelope.getDeliveryTag(),true);
            //System.out.println("批量消费者进行消息的确认-------------");
        }

    }
}
