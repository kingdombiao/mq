package com.kingdombiao.reject;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * 描述:
 * 生产者
 *
 * @author biao
 * @create 2019-09-04 17:06
 */
public class RejectProducer {

    public final static String EXCHANGE_NAME = "producer_reject";

    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        Connection connection = connectionFactory.newConnection();
        Channel channel = connection.createChannel();

        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT, true);

        String[] logLevels = {"error", "info", "warning"};

        for (String logLevel : logLevels) {
            String msg = "hello_world_" + logLevel;
            channel.basicPublish(EXCHANGE_NAME, logLevel, true, null, msg.getBytes());
            System.out.println(" Sent Message: [" + logLevel + "]:'" + msg + "'");
        }

        channel.close();
        connection.close();
    }

}
