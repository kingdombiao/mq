package com.kingdombiao.exchange.direct;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * 描述:
 * direct类型交换器的生产者
 *
 * @author biao
 * @create 2019-09-02 14:02
 */
public class DirectProducer {

    public final static String EXCHANGE_NAME="direct_logs";
    public final static String EXCHANGE_TYPE="direct";

    public static void main(String[] args) throws IOException, TimeoutException {
        //创建连接工厂
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("127.0.0.1");

        //通过连接工厂创建连接
        Connection connection = connectionFactory.newConnection();

        //通过连接创建信道
        Channel channel = connection.createChannel();

        //通过信道创建交换器
        channel.exchangeDeclare(EXCHANGE_NAME,EXCHANGE_TYPE);

        String[] logLevel={"warning","info","error"};
        for (int i=0;i<logLevel.length;i++){
            String routeKey=logLevel[i];
            String msg="hello,rabbitmq-"+i;
            //发布消息
            channel.basicPublish(EXCHANGE_NAME,routeKey,null,msg.getBytes());

            System.out.println("Sent "+ routeKey+":"+msg+ "成功");
        }
        channel.close();
        connection.close();
    }
}
