package com.kingdombiao.backupExchange;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * 描述:
 * 主消费者
 *
 * @author biao
 * @create 2019-09-04 10:59
 */
public class MainConsumer {

    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        Connection connection = connectionFactory.newConnection();
        Channel channel = connection.createChannel();

        //channel.exchangeDeclare(MainProducer.EXCHANGE_NAME,BuiltinExchangeType.DIRECT,false,false,null);

        String queue="focus_error";

        channel.queueDeclare(queue,false,false,false,null);

        channel.queueBind(queue,MainProducer.EXCHANGE_NAME,"error");

        System.out.println("************waiting for the messages****************");

        Consumer consumer=new DefaultConsumer(channel){
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope,
                                       AMQP.BasicProperties properties, byte[] body) throws IOException {

                System.out.println( "Received ["
                        + envelope.getRoutingKey() + "] "+new String(body));
            }
        };

        channel.basicConsume(queue,true,consumer);
    }
}
