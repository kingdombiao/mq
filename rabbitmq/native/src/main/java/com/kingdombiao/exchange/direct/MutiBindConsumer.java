package com.kingdombiao.exchange.direct;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * 描述:
 * 队列和交换器的多重绑定
 *
 * @author biao
 * @create 2019-09-02 15:36
 */
public class MutiBindConsumer {

    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        Connection connection = connectionFactory.newConnection();
        Channel channel = connection.createChannel();

        channel.exchangeDeclare(DirectProducer.EXCHANGE_NAME, BuiltinExchangeType.DIRECT);

        //声明一个随机队列
        String queue = channel.queueDeclare().getQueue();

        String[] logLevel = {"warning", "info", "error"};

        for (String routeKey : logLevel) {
            //队列绑定到交换器上时，是允许绑定多个路由健的，也就是所谓的“多重绑定”
            channel.queueBind(queue, DirectProducer.EXCHANGE_NAME, routeKey);
        }

        System.out.println("***********waiting for messages****************");

        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope,
                                       AMQP.BasicProperties properties, byte[] body) throws IOException {
                String message = new String(body, "UTF-8");
                System.out.println(" Received "
                        + envelope.getRoutingKey() + ":'" + message
                        + "'");
            }
        };

        channel.basicConsume(queue,true,consumer);

    }
}
