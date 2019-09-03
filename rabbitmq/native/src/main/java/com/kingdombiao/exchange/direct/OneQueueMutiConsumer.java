package com.kingdombiao.exchange.direct;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * 描述:
 * 一个队列，多个消费者，这发送的消息会在消费者之间轮询发送
 *
 * @author biao
 * @create 2019-09-02 17:04
 */
public class OneQueueMutiConsumer {

    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        Connection connection = connectionFactory.newConnection();

        //3个线程，线程之间共享队列,一个队列多个消费者
        String queue= "focusAll";

        for (int i=0;i<2;i++){
            new Thread(new ConsumerWorker(connection,queue)).start();
        }
    }

    private static class ConsumerWorker implements Runnable {
        private Connection connection;
        private String queue;

        public ConsumerWorker(Connection connection, String queue) {
            this.connection = connection;
            this.queue = queue;
        }

        @Override
        public void run() {

            try {
                Channel channel = connection.createChannel();

                //队列
                channel.queueDeclare(queue, false, false, false, null);

                //路由键
                String[] logLevel = {"warning", "info", "error"};
                for (String routeKey : logLevel) {
                    channel.queueBind(queue, DirectProducer.EXCHANGE_NAME, routeKey);
                }

                //打印消费者名字
                String consumerName = Thread.currentThread().getName() + "-consumer";


                //创建消费者
                Consumer consumer = new DefaultConsumer(channel) {
                    @Override
                    public void handleDelivery(String consumerTag, Envelope envelope,
                                               AMQP.BasicProperties properties, byte[] body) throws IOException {

                        String msg = new String(body, "utf-8");
                        System.out.println(consumerName + "-Received " + envelope.getRoutingKey() + ":" + msg);
                    }
                };
                channel.basicConsume(queue,true,consumer);
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
    }
}