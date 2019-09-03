package com.kingdombiao.exchange.fanout;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * 描述:
 * 消费者A
 *
 * @author biao
 * @create 2019-09-02 17:52
 */
public class ConsumerA {

    public static void main(String[] args) throws IOException, TimeoutException {

        ConnectionFactory connectionFactory = new ConnectionFactory();
        Connection connection = connectionFactory.newConnection();
        Channel channel = connection.createChannel();


        channel.exchangeDeclare(FanoutProducer.EXCHANGE_NAME, BuiltinExchangeType.FANOUT);

        String queue = channel.queueDeclare().getQueue();

       /* String[] logLevel = {"warning", "info", "error"};

        for (String routeKey : logLevel) {
            channel.queueBind(queue, FanoutProducer.EXCHANGE_NAME, routeKey);
        }*/

        channel.queueBind(queue, FanoutProducer.EXCHANGE_NAME, "aaa");

        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope,
                                       AMQP.BasicProperties properties, byte[] body) throws IOException {

                String message = new String(body, "utf-8");
                System.out.println(" Received " + envelope.getRoutingKey() + "':'" + message + "'");
            }
        };

        channel.basicConsume(queue, true, consumer);
    }
}
