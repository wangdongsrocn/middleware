package org.learn.rabbitmq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.Scanner;
import java.util.concurrent.TimeoutException;

public class EmitLogDirect {
    private static final String EXCHANGE_NAME = "direct_logs";
    private static String[] strings = {"black", "red", "green"};

    public static void main(String[] args) {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("localhost");
        try (Connection connection = connectionFactory.newConnection(); Channel channel = connection.createChannel();) {
            channel.exchangeDeclare(EXCHANGE_NAME, "direct");
            while (true) {
                int a = (int) (Math.random() * 10);
                if (a > 2) a = 2;
                Scanner scanner = new Scanner(System.in);
                String message = scanner.next();
                channel.basicPublish(EXCHANGE_NAME, strings[a], null, message.getBytes());
                System.out.println(strings[a] + " : " + message);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (TimeoutException e) {
            throw new RuntimeException(e);
        }
    }
}
