package org.learn.rabbitmq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.Scanner;
import java.util.concurrent.TimeoutException;

public class NewTask {
    private final static String QUEUE_NAME = "HELLO";

    public static void main(String[] args) {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("localhost");
        try (Connection connection = connectionFactory.newConnection(); Channel channel = connection.createChannel();) {
            channel.queueDeclare(QUEUE_NAME, false, false, false, null);
            while (true) {
                Scanner scanner = new Scanner(System.in);
                String message = scanner.next();
                channel.basicPublish("", QUEUE_NAME, null, message.getBytes());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (TimeoutException e) {
            throw new RuntimeException(e);
        }
    }
}
