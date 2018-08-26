package com.chnic.rabbitmq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConfirmListener;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Slf4j
public class RabbitMQProducer {

    private static final String EXCHANGE_NAME = "demo_exchange_1";

    private static final String QUEUE_NAME = "demo_queue_1";

    private static final String ROUTING_KEY = "demo_routing_key";

    public static void main(String[] args) throws IOException, TimeoutException, InterruptedException {
        log.info("RabbitMQProducer Start...");
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setVirtualHost("/");
        connectionFactory.setHost("node2");
        connectionFactory.setPort(5672);

        Connection connection = connectionFactory.newConnection();
        Channel channel = connection.createChannel();

        channel.addReturnListener((replyCode, replyText, exchange, routingKey, properties, body) -> {
            String returnMessage = new String(body);
            log.info("Basic.Return：{}", returnMessage);
        });

        channel.addConfirmListener(new ConfirmListener() {
            @Override
            public void handleAck(long deliveryTag, boolean multiple) {
                log.info("Basic.Ack：{}", deliveryTag);
            }

            @Override
            public void handleNack(long deliveryTag, boolean multiple) {
                log.info("Basic.Nack：{}", deliveryTag);
            }
        });

//        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT, true, false, Maps.newHashMap());
//        channel.queueDeclare(QUEUE_NAME, true, false, false, Maps.newHashMap());
        channel.queueBind(QUEUE_NAME, EXCHANGE_NAME, ROUTING_KEY);

        String message = "Hello World";
        String returnMessage = "Return Message";

//        AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder().deliveryMode(2).priority(1).build();
        channel.confirmSelect();
        channel.basicPublish(EXCHANGE_NAME, ROUTING_KEY, true, MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBytes());
        channel.basicPublish(EXCHANGE_NAME, ROUTING_KEY + "123", true, MessageProperties.PERSISTENT_TEXT_PLAIN, returnMessage.getBytes());


        TimeUnit.SECONDS.sleep(3);

        channel.close();
        connection.close();
        log.info("Send Success...");
    }
}
