package com.kingdombiao.reject;

import com.kingdombiao.transaction.ProducerTrancation;
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
public class NormalConsumer {

    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        Connection connection = connectionFactory.newConnection();
        Channel channel = connection.createChannel();

        channel.exchangeDeclare(RejectProducer.EXCHANGE_NAME, BuiltinExchangeType.DIRECT,true);

        String queue="q_reject";
        channel.queueDeclare(queue,true,false,false,null);

        channel.queueBind(queue,RejectProducer.EXCHANGE_NAME,"error");

        System.out.println("************Waiting for the messages************");

        // 创建队列消费者
        final Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag,
                                       Envelope envelope,
                                       AMQP.BasicProperties properties,
                                       byte[] body) throws IOException {


                    String message = new String(body, "UTF-8");
                    //记录日志到文件：
                    System.out.println( "Received ["+ envelope.getRoutingKey()
                            + "] "+message);

                    channel.basicAck(envelope.getDeliveryTag(),false);


            }
        };
        channel.basicConsume(queue, false, consumer);

    }

}
