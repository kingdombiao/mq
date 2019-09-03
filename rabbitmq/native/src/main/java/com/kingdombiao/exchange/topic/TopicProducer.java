package com.kingdombiao.exchange.topic;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * 描述:
 * topic类型交换器的生产者
 *
 * @author biao
 * @create 2019-09-03 9:33
 */
public class TopicProducer {

    public final static String EXCHANGE_NAME = "topic_logs";

    public static void main(String[] args) throws IOException, TimeoutException {

        ConnectionFactory connectionFactory = new ConnectionFactory();
        Connection connection = connectionFactory.newConnection();

        Channel channel = connection.createChannel();

        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.TOPIC);

        String[] servers = {"A", "B", "C"};
        for (String server : servers) {
            String[] modules = {"user", "order", "email"};
            for (String module : modules) {
                String[] logLevels = {"warning", "info", "error"};
                for (String loglevel : logLevels) {
                    //构建消息
                    String msg = "hello topic->[" + server + "," + module + "," + loglevel + "]";
                    String routeKey = server + "." + module + "." + loglevel;
                    channel.basicPublish(EXCHANGE_NAME, routeKey, null, msg.getBytes());
                    System.out.println("send [" + routeKey + "]: " + msg + " success");
                }
            }
        }
        channel.close();
        connection.close();
    }
}
