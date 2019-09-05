package com.kingdombiao.mandatory;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * 描述:
 * direct类型交换器的生产者
 *
 * @author biao
 * @create 2019-09-02 14:02
 */
public class MandatoryProducer {

    public final static String EXCHANGE_NAME="direct_logs";
    public final static String EXCHANGE_TYPE="direct";

    public static void main(String[] args) throws IOException, TimeoutException, InterruptedException {
        //创建连接工厂
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("127.0.0.1");

        //通过连接工厂创建连接
        Connection connection = connectionFactory.newConnection();

        //通过连接创建信道
        Channel channel = connection.createChannel();

        //通过信道创建交换器
        channel.exchangeDeclare(EXCHANGE_NAME,EXCHANGE_TYPE);


        //监听连接关闭
        connection.addShutdownListener(new ShutdownListener() {
            @Override
            public void shutdownCompleted(ShutdownSignalException cause) {
                System.out.println("监听连接关闭---->"+cause.getReason()+"------>"+cause.getMessage());
            }
        });

        //监听信道关闭
        channel.addShutdownListener(new ShutdownListener() {
            @Override
            public void shutdownCompleted(ShutdownSignalException cause) {
                System.out.println("监听信道关闭---->"+cause.getReason()+"------>"+cause.getMessage());
            }
        });

        channel.addReturnListener(new ReturnListener() {
            @Override
            public void handleReturn(int replyCode, String replyText, String exchange, String routingKey,
                                     AMQP.BasicProperties properties, byte[] body) throws IOException {

                String message = new String(body);
                System.out.println("返回的replyText ："+replyText);
                System.out.println("返回的exchange ："+exchange);
                System.out.println("返回的routingKey ："+routingKey);
                System.out.println("返回的message ："+message);
                System.out.println("*********************************************");
            }
        });

        channel.addReturnListener(new ReturnCallback() {
            @Override
            public void handle(Return returnMessage) {
                String message = new String(returnMessage.getBody());
                System.out.println("返回的replyText--> ："+returnMessage.getReplyText());
                System.out.println("返回的exchange--> ："+returnMessage.getExchange());
                System.out.println("返回的routingKey--> ："+returnMessage.getRoutingKey());
                System.out.println("返回的message--> ："+message);

                System.out.println("*********************************************");
            }
        });


        String[] logLevel={"warning","info","error"};
        for (int i=0;i<logLevel.length;i++){
            String routeKey=logLevel[i];
            String msg="hello,rabbitmq-"+i;
            //发布消息 同时设置mandatory=true
            channel.basicPublish(EXCHANGE_NAME,routeKey,true,null,msg.getBytes());

            System.out.println("Sent "+ routeKey+":"+msg+ "成功");

            Thread.sleep(2000);
        }
        channel.close();
        connection.close();
    }
}
