package org.learn.rabbitmq;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.Scanner;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

public class RPCClient implements AutoCloseable {
    private Connection connection;
    private Channel channel;
    private static final String QUEUE_NAME = "rpc_queue";

    public RPCClient() throws IOException, TimeoutException {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("localhost");
        connection = connectionFactory.newConnection();
        channel = connection.createChannel();
    }

    public String call(String message) throws IOException, ExecutionException, InterruptedException {
        final String corrId = UUID.randomUUID().toString();
        String replyQueueName = channel.queueDeclare().getQueue();
        AMQP.BasicProperties basicProperties = new AMQP.BasicProperties.Builder().correlationId(corrId).replyTo(replyQueueName).build();
        channel.basicPublish("", QUEUE_NAME, basicProperties, message.getBytes());
        final CompletableFuture<String> completableFuture = new CompletableFuture<>();
        String ctag = channel.basicConsume(replyQueueName, true, (consumerTag, delivery) -> {
            if (delivery.getProperties().getCorrelationId().equals(corrId)) {
                completableFuture.complete(new String(delivery.getBody(), "UTF-8"));
            }
        }, consumerTag -> {
        });
        String result = completableFuture.get();
        channel.basicCancel(ctag);
        return result;
    }

    @Override
    public void close() throws Exception {
        connection.close();
    }

    public static void main(String[] args) throws Exception {
        try (RPCClient rpcClient = new RPCClient()) {
            while (true) {
                Scanner scanner = new Scanner(System.in);
                String in = scanner.next();
                String response = rpcClient.call(in);
                System.out.println("response: " + response);
            }
        }
    }
}
