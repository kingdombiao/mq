package com.kingdombiao.dlx;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 * 描述:
 * 消费者
 *
 * @author biao
 * @create 2019-09-05 11:08
 */
public class NormalConsumer {
    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        Connection connection = connectionFactory.newConnection();
        Channel channel = connection.createChannel();

        channel.exchangeDeclare(DlxProducer.NORMAL_EXCHANGE_NAME, BuiltinExchangeType.TOPIC,true,false,null);

        Map<String,Object> argsMap=new HashMap<>();
        //argsMap.put("x-message-ttl",5000);//设置消息有效期5秒，过期后变成死信消息，然后进入DLX
        argsMap.put("x-dead-letter-exchange",DlxProducer.DLX_EXCHANGE_NAME);
        argsMap.put("x-dead-letter-routing-key","dlx.reject");//设置DLX的路由键(可以不设置)

        String normal_queue="normal_queue";
        channel.queueDeclare(normal_queue,true,false,false,argsMap);
        channel.queueBind(normal_queue,DlxProducer.NORMAL_EXCHANGE_NAME,"#");

        Consumer consumer=new DefaultConsumer(channel){
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope,
                                       AMQP.BasicProperties properties, byte[] body) throws IOException {

                String msg=new String(body,"utf-8");

                if("error".equals(envelope.getRoutingKey())){
                    System.out.println("Received[" +envelope.getRoutingKey() +"]"+msg);
                    channel.basicAck(envelope.getDeliveryTag(),false);
                }else {
                    System.out.println("rejected [" +envelope.getRoutingKey() +"]"+msg);
                    channel.basicReject(envelope.getDeliveryTag(),false);
                }
            }
        };

        channel.basicConsume(normal_queue,false,consumer);
    }
}
