package com.kingdombiao.qos;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * 描述:
 * 消費者
 *
 * @author biao
 * @create 2019-09-04 13:49
 */
public class QosConsumerMain {

    public static void main(String[] args) throws IOException, TimeoutException {

        ConnectionFactory connectionFactory = new ConnectionFactory();
        Connection connection = connectionFactory.newConnection();
        Channel channel = connection.createChannel();

        channel.exchangeDeclare(QosProducer.EXCHANGE_NAME, BuiltinExchangeType.DIRECT);

        String queue = channel.queueDeclare().getQueue();

        channel.queueBind(queue,QosProducer.EXCHANGE_NAME,"error");

        System.out.println("************waiting for the message********");

        Consumer consumer=new  DefaultConsumer(channel){
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope,
                                       AMQP.BasicProperties properties, byte[] body) throws IOException {

                System.out.println("Received["+envelope.getRoutingKey()
                        +"]"+new String(body));

                //channel.basicAck(envelope.getDeliveryTag(),false);
            }
        };

        //也就是说，整个通道加起来最多允许10条未确认的消息，每个消费者则最多有9条消息。
        channel.basicQos(9,false); //Per consumer limit
        channel.basicQos(10,true); //Per channel limit

        channel.basicConsume(queue,false,consumer);
        channel.basicConsume(queue,false,new BatchAckConsumer(channel));
    }


}
