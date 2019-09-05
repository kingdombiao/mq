package com.kingdombiao.backupExchange;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * 描述:
 * 生产者
 *
 * @author biao
 * @create 2019-09-04 10:49
 */
public class MainProducer {
    public final static String EXCHANGE_NAME = "main_exchange";
    public final static String BAK_EXCHANGE_NAME = "back_exchange";

    public static void main(String[] args) throws IOException, TimeoutException, InterruptedException {

        ConnectionFactory connectionFactory = new ConnectionFactory();
        Connection connection = connectionFactory.newConnection();

        Channel channel = connection.createChannel();

        //声明备用交换器
        Map<String, Object> argsMap = new HashMap<>();
        argsMap.put("alternate-exchange", BAK_EXCHANGE_NAME);


        //创建备用交换器
        channel.exchangeDeclare(BAK_EXCHANGE_NAME, BuiltinExchangeType.FANOUT, true, false, null);

        //创建主交换器
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT, false, false, argsMap);



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

        String[] logLevels = {"error", "info", "warning"};

        for (String logLevel : logLevels) {
            String msg = "hello " + logLevel;
            channel.basicPublish(EXCHANGE_NAME, logLevel,true, null, msg.getBytes());
            System.out.println("send [" + logLevel + "]:" + msg + " success");
        }

        TimeUnit.SECONDS.sleep(5);

        channel.close();
        connection.close();
    }

}
