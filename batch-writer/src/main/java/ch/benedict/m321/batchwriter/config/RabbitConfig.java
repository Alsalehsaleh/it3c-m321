package ch.benedict.m321.batchwriter.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Alles, was der batch-writer beim Start vom Broker braucht.
 *
 * Die Queues legt eigentlich der chat-service an. Wir deklarieren sie trotzdem, weil der
 * batch-writer auch dann starten soll, wenn der chat-service noch nie gesendet hat — ohne
 * Queue könnte sich der Listener nirgends anhängen. Die Eigenschaften müssen exakt
 * dieselben sein wie im chat-service, sonst lehnt RabbitMQ mit PRECONDITION_FAILED ab
 * (Spec 2.1).
 */
@Configuration
public class RabbitConfig {

    /** Ein voller Stapel: 500 Nachrichten (PLANUNG.md §3.6). */
    private static final int BATCH_SIZE = 500;

    /** Spätestens nach 200 ms wird geschrieben, auch wenn der Stapel nicht voll ist. */
    private static final long BATCH_TIMEOUT_MILLIS = 200;

    /**
     * Der Schreibweg, gleich wie im chat-service: durable, und was abgelehnt wird, geht über
     * den Standard-Exchange ("") in die Dead-Letter-Queue.
     */
    @Bean
    public Queue persistQueue() {
        return QueueBuilder.durable(QueueNames.PERSIST_QUEUE)
                .deadLetterExchange("")
                .deadLetterRoutingKey(QueueNames.DEAD_LETTER_QUEUE)
                .build();
    }

    /** Das Abstellgleis für Nachrichten, die selbst kaputt sind (Spec 3.3). */
    @Bean
    public Queue deadLetterQueue() {
        return QueueBuilder.durable(QueueNames.DEAD_LETTER_QUEUE).build();
    }

    /**
     * Nachrichten kommen als JSON an. Der ObjectMapper von Spring Boot kann Instant lesen,
     * auch mit den neun Nachkommastellen aus Spec 2.2.
     */
    @Bean
    public MessageConverter jsonMessageConverter(ObjectMapper objectMapper) {
        return new Jackson2JsonMessageConverter(objectMapper);
    }

    /**
     * Stellt ein, wie der Listener-Container Stapel bildet.
     *
     * Er sammelt bis zu 500 Nachrichten oder wartet höchstens 200 ms und ruft dann die
     * Listener-Methode einmal mit der ganzen Liste auf. Der Prefetch muss mindestens so
     * gross sein wie der Stapel: Er begrenzt, wie viele unbestätigte Nachrichten RabbitMQ
     * herausgibt. Wäre er kleiner, würde der Stapel nie voll (Spec 3.1).
     */
    @Bean
    public SimpleRabbitListenerContainerFactory batchContainerFactory(
            ConnectionFactory connectionFactory, MessageConverter jsonMessageConverter) {
        SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
        factory.setConnectionFactory(connectionFactory);
        factory.setMessageConverter(jsonMessageConverter);
        factory.setBatchListener(true);
        factory.setConsumerBatchEnabled(true);
        factory.setBatchSize(BATCH_SIZE);
        factory.setPrefetchCount(BATCH_SIZE);
        factory.setReceiveTimeout(BATCH_TIMEOUT_MILLIS);
        factory.setBatchReceiveTimeout(BATCH_TIMEOUT_MILLIS);
        // Was die Listener-Methode mit einer Exception verlässt, gilt als kaputte Nachricht:
        // Der Stapel geht in die DLQ, statt endlos neu zugestellt zu werden. Einen
        // Datenbankausfall fängt der Listener deshalb selbst ab (Spec 3.2.1).
        factory.setDefaultRequeueRejected(false);
        return factory;
    }
}
