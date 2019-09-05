package com.kingdombiao.qos;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.sun.org.apache.bcel.internal.generic.NEW;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * 描述:
 * 生产者
 *
 * @author biao
 * @create 2019-09-04 13:42
 */
public class QosProducer {

    public final static String EXCHANGE_NAME = "direct_logs";

    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        Connection connection = connectionFactory.newConnection();
        Channel channel = connection.createChannel();

        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT);


        for(int i=0;i<300;i++){

            String msg="hello_world_"+i;
            if(i==299){
                msg="stop";
            }

            channel.basicPublish(EXCHANGE_NAME,"error",null,msg.getBytes());
            System.out.println(" Sent 'error':'"
                    + msg + "'");
        }

        channel.close();
        connection.close();
    }

}
