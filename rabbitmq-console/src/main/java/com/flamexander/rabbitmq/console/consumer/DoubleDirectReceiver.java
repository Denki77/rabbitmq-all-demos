package com.flamexander.rabbitmq.console.consumer;

import com.rabbitmq.client.*;

import java.util.Scanner;

public class DoubleDirectReceiver {
    private static final String EXCHANGE_NAME = "DoubleDirect";

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT);

        String queueName = channel.queueDeclare().getQueue();
        System.out.println("My queue name: " + queueName);

        Scanner scanner = new Scanner(System.in);
        String getMeQueueName = null;

        while (getMeQueueName == null) {

            System.out.println("Get me queue name (format: 'set_topic <queue name>')");
            String cmd = scanner.next();

            if (cmd.equals("set_topic")) {
                getMeQueueName = scanner.next();
                if (getMeQueueName.trim().isEmpty()) {
                    getMeQueueName = null;
                    System.out.println("Not empty queue name!");
                }
            } else {
                System.out.println("Try again - format: 'set_topic <queue name>'");
            }
        }

        channel.queueBind(queueName, EXCHANGE_NAME, getMeQueueName);

        System.out.println(" [*] Listen message for '" + getMeQueueName + "':");

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            System.out.println(" [x] New topic for you '" + message + "'");
        };

        channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {
        });
    }
}
