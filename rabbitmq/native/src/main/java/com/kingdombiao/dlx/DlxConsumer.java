package com.kingdombiao.dlx;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;


/**
 * 描述:
 * 死信消费者 消费死信队列
 *
 * @author biao
 * @create 2019-09-05 13:32
 */
public class DlxConsumer {
    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        Connection connection = connectionFactory.newConnection();
        Channel channel = connection.createChannel();

        //声明死信交换器
        channel.exchangeDeclare(DlxProducer.DLX_EXCHANGE_NAME, BuiltinExchangeType.TOPIC, true, false, null);

        //声明死信队列
        String dlx_queue = "dlx_queue";
        channel.queueDeclare(dlx_queue, true, false, false, null);

        //队列绑定到交换器上
        channel.queueBind(dlx_queue, DlxProducer.DLX_EXCHANGE_NAME, "dlx.reject");

        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope,
                                       AMQP.BasicProperties properties, byte[] body) throws IOException {

                String msg = new String(body, "utf-8");

                System.out.println("Received[" + envelope.getRoutingKey() + "]" + msg);
                channel.basicAck(envelope.getDeliveryTag(), false);
            }
        };

        channel.basicConsume(dlx_queue, false, consumer);
    }
}
