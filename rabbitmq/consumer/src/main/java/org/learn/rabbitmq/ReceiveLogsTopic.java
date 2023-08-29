package org.learn.rabbitmq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class ReceiveLogsTopic {
    private static final String EXCHANGE_NAME = "topic_logs";

    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("localhost");
        Connection connection = connectionFactory.newConnection();
        Channel channel = connection.createChannel();
        channel.exchangeDeclare(EXCHANGE_NAME, "topic");
        String queueName = channel.queueDeclare().getQueue();
        // 针对第三个参数，可进行多次绑定
        channel.queueBind(queueName, EXCHANGE_NAME, args[0]);
        channel.basicQos(1);
        DeliverCallback deliverCallback = (consumerTag, deliver) -> {
            String message = new String(deliver.getBody(), "UTF-8");
            try {
                doWork(message);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } finally {
                System.out.println("done: " + args[0] + " : " + message);
                channel.basicAck(deliver.getEnvelope().getDeliveryTag(), false);
            }
        };

        channel.basicConsume(queueName, false, deliverCallback, consumerTag -> {
        });
    }

    private static void doWork(String message) throws InterruptedException {
        for (char ch : message.toCharArray()) {
            if (ch == '.') Thread.sleep(1000);
        }
    }
}
