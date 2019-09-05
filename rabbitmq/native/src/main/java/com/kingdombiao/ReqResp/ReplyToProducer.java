package com.kingdombiao.ReqResp;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

/**
 * 描述:
 * 消息的属性的控制
 *
 * @author biao
 * @create 2019-09-05 16:29
 */
public class ReplyToProducer {

    public final static String EXCHANGE_NAME = "reply_to";

    public static void main(String[] args) throws IOException, TimeoutException {

        ConnectionFactory connectionFactory = new ConnectionFactory();
        Connection connection = connectionFactory.newConnection();
        Channel channel = connection.createChannel();

        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT,false);

        //创建响应队列
        String RespQueue = channel.queueDeclare().getQueue();

        AMQP.BasicProperties properties = new AMQP.BasicProperties().builder().replyTo(RespQueue).messageId(UUID.randomUUID().toString()).build();

        Consumer consumer=new DefaultConsumer(channel){
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope,
                                       AMQP.BasicProperties properties, byte[] body) throws IOException {

                String message = new String(body, "UTF-8");
                System.out.println("response: Received["+envelope.getRoutingKey()
                        +"]"+message);
            }
        };

        channel.basicConsume(RespQueue,true,consumer);



        String msg="hello rabbitMq,Please reply upon receipt";

        channel.basicPublish(EXCHANGE_NAME,"error",properties,msg.getBytes());

    }
}
