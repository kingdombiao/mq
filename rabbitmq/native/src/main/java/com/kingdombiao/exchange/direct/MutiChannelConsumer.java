package com.kingdombiao.exchange.direct;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * 描述:
 * 一个连接多个信道
 *
 * @author biao
 * @create 2019-09-02 16:43
 */
public class MutiChannelConsumer {


    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        Connection connection = connectionFactory.newConnection();
        for(int i=0;i<3;i++){
            new Thread(new ConsumerWorker(connection)).start();
        }
    }



    private static class  ConsumerWorker implements Runnable{

        private final Connection connection;

        public ConsumerWorker(Connection connection) {
            this.connection = connection;
        }

        @Override
        public void run() {

            try {
                Channel channel = connection.createChannel();

                channel.exchangeDeclare(DirectProducer.EXCHANGE_NAME, BuiltinExchangeType.DIRECT);

                //声明一个随机队列
                String queue = channel.queueDeclare().getQueue();

                String[] logLevel = {"warning", "info", "error"};
                for (String routeKey : logLevel) {
                    channel.queueBind(queue,DirectProducer.EXCHANGE_NAME,routeKey);
                }

                //打印消费者名字
                String consumerName=Thread.currentThread().getName()+"consumer";
                System.out.println("["+consumerName+"] Waiting for messages:");

                //创建消费者
                Consumer consumer = new DefaultConsumer(channel) {

                    @Override
                    public void handleDelivery(String consumerTag, Envelope envelope,
                                               AMQP.BasicProperties properties, byte[] body) throws IOException {

                        String msg=new String(body,"utf-8");
                        System.out.println(consumerName+"-Received "+ envelope.getRoutingKey()+ ":"+msg);
                    }
                };

                //消费者在指定的队列上消费信息
                channel.basicConsume(queue,true,consumer);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

}
