package com.kingdombiao.transaction;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * 描述:
 * 消费者
 *
 * @author biao
 * @create 2019-09-04 17:19
 */
public class ConsumerTrancation {

    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        Connection connection = connectionFactory.newConnection();
        Channel channel = connection.createChannel();

        channel.exchangeDeclare(ProducerTrancation.EXCHANGE_NAME, BuiltinExchangeType.DIRECT,true);

        String queue="q_trancation";
        channel.queueDeclare(queue,true,false,false,null);

        channel.queueBind(queue,ProducerTrancation.EXCHANGE_NAME,"error");

        System.out.println("************Waiting for the messages************");

        // 创建队列消费者
        final Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag,
                                       Envelope envelope,
                                       AMQP.BasicProperties properties,
                                       byte[] body) throws IOException {

                try {
                    channel.txSelect();

                    String message = new String(body, "UTF-8");
                    //记录日志到文件：
                    System.out.println( "Received ["+ envelope.getRoutingKey()
                            + "] "+message);

                    channel.basicAck(envelope.getDeliveryTag(),false);

                    int a=1/0;

                    channel.txCommit();

                } catch (Exception e) {
                    e.printStackTrace();
                    channel.txRollback();

                }
            }
        };
        channel.basicConsume(queue, false, consumer);

    }

}
