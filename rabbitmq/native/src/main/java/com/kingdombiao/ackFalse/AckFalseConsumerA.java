package com.kingdombiao.ackFalse;

import com.kingdombiao.exchange.direct.DirectProducer;
import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * 描述:
 * 消费者 【不自动确认，也不手动确认】
 *
 * @author biao
 * @create 2019-09-02 15:06
 */
public class AckFalseConsumerA {

    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory connectionFactory = new ConnectionFactory();

        Connection connection = connectionFactory.newConnection();

        Channel channel = connection.createChannel();

        channel.exchangeDeclare(DirectProducer.EXCHANGE_NAME,"direct");

        //声明一个队列
        String queue="focus_error";
        channel.queueDeclare(queue,false,false,false,null);

        //将队列通过路由键和交换器进行绑定
        String routeKey="error";
        channel.queueBind(queue,DirectProducer.EXCHANGE_NAME,routeKey);

        System.out.println("wait for receiving message...........");

        //声明一个消费者
        Consumer consumer = new DefaultConsumer(channel){

            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {

                String msg=new String(body,"utf-8");
                System.out.println("Received["+envelope.getRoutingKey()+"]"+msg);

                // TODO: 2019/9/3  不手动确认看一下效果
            }
        };

        channel.basicConsume(queue,false,consumer);

    }
}
