package com.kingdombiao.exchange.fanout;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * 描述:
 * fanout类型交换器的生产者
 *
 * @author biao
 * @create 2019-09-02 17:38
 */
public class FanoutProducer {

    public final static String EXCHANGE_NAME="fanout_logs";

    public static void main(String[] args) throws IOException, TimeoutException {

        ConnectionFactory connectionFactory = new ConnectionFactory();
        Connection connection = connectionFactory.newConnection();

        Channel channel = connection.createChannel();

        //声明交换器
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.FANOUT);

        //声明队列
        String queue="producer_create";
        channel.queueDeclare(queue,false,false,false,null);

        channel.queueBind(queue,EXCHANGE_NAME,"no_exist_route_Key");

        //路由键
        String[] logLevel={"warning","info","error"};
        for (String routeKey : logLevel) {
            String msg="hello world "+routeKey;
            channel.basicPublish(EXCHANGE_NAME,routeKey,null,msg.getBytes());
            System.out.println("send ["+msg+"] success");
        }

        channel.close();
        connection.close();
    }
}
