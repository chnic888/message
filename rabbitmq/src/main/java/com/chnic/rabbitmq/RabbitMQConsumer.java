package com.chnic.rabbitmq;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Address;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Slf4j
public class RabbitMQConsumer {

    private static final String QUEUE_NAME = "demo_queue_1";

    public static void main(String[] args) throws IOException, TimeoutException, InterruptedException {
        log.info("RabbitMQConsumer Start...");
        Address[] addresses = new Address[]{new Address("node2", 5672)};
        ConnectionFactory connectionFactory = new ConnectionFactory();
        Connection connection = connectionFactory.newConnection(addresses);
        final Channel channel = connection.createChannel();
        channel.basicQos(64);

        Random random = new Random();

        channel.basicConsume(QUEUE_NAME, new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                log.info("Receive Message: {}", new String(body));
                try {
                    TimeUnit.SECONDS.sleep(random.nextInt(5));
                } catch (InterruptedException e) {
                    log.error(e.getMessage());
                }
                channel.basicAck(envelope.getDeliveryTag(), false);
            }
        });

        TimeUnit.SECONDS.sleep(30000);
        channel.close();
        connection.close();
    }
}
