package com.kingdombiao.producerConfirm;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * 描述:
 * Confirm的三种实现方式：
 *
 * 方式一：channel.waitForConfirms()普通发送方确认模式；
 *
 * 方式二：channel.waitForConfirmsOrDie()批量确认模式；
 *
 * 方式三：channel.addConfirmListener()异步监听发送方确认模式；
 *
 * @author biao
 * @create 2019-09-04 10:07
 */
public class ProducerAsyncConfirm {
    public final static String EXCHANGE_NAME="producer_confirm";

    private final static String ROUTE_KEY="error";

    public static void main(String[] args) throws IOException, TimeoutException, InterruptedException {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        Connection connection = connectionFactory.newConnection();
        Channel channel = connection.createChannel();

        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT);

        channel.addReturnListener(new ReturnListener() {
            @Override
            public void handleReturn(int replyCode, String replyText, String exchange,
                                     String routingKey, AMQP.BasicProperties properties, byte[] body) throws IOException {

                System.out.println("返回的replyText ："+replyText);
                System.out.println("返回的exchange ："+exchange);
                System.out.println("返回的routingKey ："+routingKey);
                System.out.println("返回的message ："+new String(body));
                System.out.println("*********************************************");
            }
        });

        //开启发送者确认模式
        channel.confirmSelect();

        channel.addConfirmListener(new ConfirmListener() {
            @Override
            public void handleAck(long deliveryTag, boolean multiple) throws IOException {
                System.out.println("确认成功  deliveryTag:"+deliveryTag
                        +",multiple:"+multiple);
            }

            @Override
            public void handleNack(long deliveryTag, boolean multiple) throws IOException {
                System.out.println("确认失败  deliveryTag:"+deliveryTag
                        +",multiple:"+multiple);
            }
        });

        for (int i=0;i<2;i++){
            String msg="helle_world_"+(i+1);
            channel.basicPublish(EXCHANGE_NAME,ROUTE_KEY,true,null,msg.getBytes());
        }

        TimeUnit.SECONDS.sleep(2);

        channel.close();
        connection.close();
    }


}
