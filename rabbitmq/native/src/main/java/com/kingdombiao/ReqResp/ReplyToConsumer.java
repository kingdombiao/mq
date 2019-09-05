package com.kingdombiao.ReqResp;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * 描述:
 * 消息的属性的控制
 *
 * @author biao
 * @create 2019-09-05 16:40
 */
public class ReplyToConsumer {

    public static void main(String[] args) throws IOException, TimeoutException {

        ConnectionFactory connectionFactory = new ConnectionFactory();
        Connection connection = connectionFactory.newConnection();
        Channel channel = connection.createChannel();

        channel.exchangeDeclare(ReplyToProducer.EXCHANGE_NAME, BuiltinExchangeType.DIRECT, false);

        //创建响应队列
        String queue = channel.queueDeclare().getQueue();

        channel.queueBind(queue, ReplyToProducer.EXCHANGE_NAME, "error");

        System.out.println("*************waiting for messages**************");

        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope,
                                       AMQP.BasicProperties properties, byte[] body) throws IOException {

                String msg = new String(body, "utf-8");
                System.out.println("Received[" + envelope.getRoutingKey() + "]" + msg);

                AMQP.BasicProperties respProperties = new AMQP.BasicProperties().builder()
                        .replyTo(properties.getReplyTo())
                        .contentEncoding(properties.getMessageId())
                        .build();

                channel.basicPublish("",respProperties.getReplyTo(),respProperties,("已收到["+msg+"]").getBytes("utf-8"));

            }
        };

        channel.basicConsume(queue,true,consumer);

    }
}
