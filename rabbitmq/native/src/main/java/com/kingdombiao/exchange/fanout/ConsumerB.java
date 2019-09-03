package com.kingdombiao.exchange.fanout;

import com.kingdombiao.exchange.direct.DirectProducer;
import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * 描述:
 * ${DESCRIPTION}
 *
 * @author unovo
 * @create 2019-09-02 18:13
 */
public class ConsumerB {
    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        Connection connection = connectionFactory.newConnection();

        Channel channel = connection.createChannel();

        channel.exchangeDeclare(DirectProducer.EXCHANGE_NAME, BuiltinExchangeType.FANOUT);

        String queue=channel.queueDeclare().getQueue();

        channel.queueBind(queue,DirectProducer.EXCHANGE_NAME,"bbbb");


        final Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope,
                                       AMQP.BasicProperties properties, byte[] body)
                    throws IOException {
                String message = new String(body, "UTF-8");
                System.out.println( "Received ["+ envelope.getRoutingKey() + "] "+message);
            }
        };
        channel.basicConsume(queue, true, consumer);


    }

}
