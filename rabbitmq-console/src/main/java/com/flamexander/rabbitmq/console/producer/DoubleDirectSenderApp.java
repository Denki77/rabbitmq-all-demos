package com.flamexander.rabbitmq.console.producer;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.util.Scanner;


public class DoubleDirectSenderApp {
    private static final String EXCHANGE_NAME = "DoubleDirect";

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {
            channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT);

            Scanner scanner = new Scanner(System.in);
            String queue_name = "";
            String messageForSend = "";

            while (!queue_name.equals("exit")) {

                System.out.println("Get message for your subscriber (format: '<queue name> some message')");
                queue_name = scanner.next();

                if (queue_name.trim().equals("")) {
                    System.out.println("Not empty queue name!");
                } else if(!queue_name.equals("exit")) {
                    messageForSend = scanner.nextLine().trim();
                    channel.basicPublish(EXCHANGE_NAME, queue_name.trim(), null, messageForSend.getBytes("UTF-8"));
                    System.out.println("You send: '" + messageForSend + "' -> for " + queue_name.trim() + " queue.");
                }
            }

            System.out.println("OK");
        }
    }
}