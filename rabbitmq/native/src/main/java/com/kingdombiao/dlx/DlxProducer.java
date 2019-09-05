package com.kingdombiao.dlx;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 * 描述:
 * 生产者
 *
 * @author biao
 * @create 2019-09-05 10:35
 */
public class DlxProducer {

    public final static String NORMAL_EXCHANGE_NAME="normal_ex";
    public final static String DLX_EXCHANGE_NAME="dlx_ex";

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
        channel.queueBind(dlx_queue, DlxProducer.DLX_EXCHANGE_NAME, "dlx.*");


        channel.exchangeDeclare(NORMAL_EXCHANGE_NAME, BuiltinExchangeType.TOPIC,true,false,null);

        Map<String,Object> argsMap=new HashMap<>();
        argsMap.put("x-message-ttl",5000);//设置消息有效期5秒，过期后变成死信消息，然后进入DLX
        argsMap.put("x-dead-letter-exchange",DlxProducer.DLX_EXCHANGE_NAME);
        argsMap.put("x-dead-letter-routing-key","dlx.reject");//设置DLX的路由键(可以不设置)

        String normal_queue="normal_queue";
        channel.queueDeclare(normal_queue,true,false,false,argsMap);
        channel.queueBind(normal_queue,DlxProducer.NORMAL_EXCHANGE_NAME,"#");

        String[] logLevels={"error","info","warning"};
        for (String logLevel : logLevels) {
            String msg="hello rabbitMq ["+logLevel+"]";
            channel.basicPublish(NORMAL_EXCHANGE_NAME,logLevel, MessageProperties.PERSISTENT_TEXT_PLAIN,msg.getBytes());
            System.out.println("send msg:["+msg+"] success");
        }

        channel.close();
        connection.close();

    }




}
