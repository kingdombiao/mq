package com.kingdombiao.exchange.topic;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * 描述:
 * 关注所有服务器发送的email的所有日志
 *
 * @author biao
 * @create 2019-09-03 11:18
 */
public class EmailAllConsumer {

    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        Connection connection = connectionFactory.newConnection();
        Channel channel = connection.createChannel();

        //声明一个交换器
        channel.exchangeDeclare(TopicProducer.EXCHANGE_NAME, BuiltinExchangeType.TOPIC);

        //声明一个随机队列
        String queue = channel.queueDeclare().getQueue();

        //将队列通过路由键和交换器进行绑定
        channel.queueBind(queue, TopicProducer.EXCHANGE_NAME, "*.email.*");

        System.out.println("****************  Waiting for messages: *********************");

        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope,
                                       AMQP.BasicProperties properties, byte[] body) throws IOException {

                String message = new String(body, "UTF-8");
                System.out.println(" Consumer Received "
                        + envelope.getRoutingKey() + "':'" + message + "'");
            }
        };

        channel.basicConsume(queue, true, consumer);
    }
}
