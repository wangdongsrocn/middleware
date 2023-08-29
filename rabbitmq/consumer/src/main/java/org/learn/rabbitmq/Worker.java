package org.learn.rabbitmq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class Worker {
    private final static String QUEUE_NAME = "HELLO";

    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("localhost");
        Connection connection = connectionFactory.newConnection();
        Channel channel = connection.createChannel();
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
        channel.basicQos(1);
        DeliverCallback deliverCallback = (consumerTag, deliver) -> {
            String message = new String(deliver.getBody(), "UTF-8");
            try {
                doWork(message);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } finally {
                System.out.println("done: " + message);
                channel.basicAck(deliver.getEnvelope().getDeliveryTag(), false);
            }
        };

        channel.basicConsume(QUEUE_NAME, false, deliverCallback, consumerTag -> {
        });
    }

    private static void doWork(String message) throws InterruptedException {
        for (char ch : message.toCharArray()) {
            if (ch == '.') Thread.sleep(1000);
        }
    }
}
