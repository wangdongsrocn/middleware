package org.learn.rabbitmq;

import com.rabbitmq.client.*;
import com.rabbitmq.client.impl.AMQBasicProperties;

import java.io.IOException;
import java.text.DateFormat;
import java.util.Date;
import java.util.concurrent.TimeoutException;

public class RPCServer {
    private static final String QUEUE_NAME = "rpc_queue";

    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("localhost");
        Connection connection = connectionFactory.newConnection();
        Channel channel = connection.createChannel();
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
        channel.queuePurge(QUEUE_NAME);
        channel.basicQos(1);
        System.out.println("Awaiting RPC request!");
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            AMQP.BasicProperties basicProperties = new AMQP.BasicProperties.Builder().correlationId(delivery.getProperties().getCorrelationId()).build();
            String response = "";
            try {
                String message = new String(delivery.getBody(), "UTF-8");
                System.out.println("received request: " + message);
                response = message + " " + new Date().toString();
                System.out.println("return response: " + response);
            } catch (RuntimeException exception) {
                System.out.println(exception.getStackTrace());
            } finally {
                channel.basicPublish("", delivery.getProperties().getReplyTo(), basicProperties, response.getBytes("UTF-8"));
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            }
        };
        channel.basicConsume(QUEUE_NAME, false, deliverCallback, consumerTag -> {
        });
    }
}
