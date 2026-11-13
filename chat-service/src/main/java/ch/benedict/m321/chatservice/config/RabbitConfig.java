package ch.benedict.m321.chatservice.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.FanoutExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.UUID;

/**
 * Legt beim Start alles an, was der Broker braucht.
 *
 * Spring meldet diese Beans beim Verbindungsaufbau an RabbitMQ. Existiert
 * eine Queue schon, passiert nichts — das Anlegen ist wiederholbar.
 */
@Configuration
@Slf4j
public class RabbitConfig {

    /**
     * Wie lange eine unbenutzte Zustell-Queue überlebt, in Millisekunden. Nur
     * das Sicherheitsnetz für eine Queue, die nie einen Consumer hatte; sonst
     * räumt auto-delete auf (Spec 3.2.2).
     */
    private static final int DELIVERY_QUEUE_EXPIRES_MILLIS = 10000;

    /**
     * Höchstens so viele wartende Nachrichten. Mit dem Prefetch von 50 sind es
     * in der Messung höchstens 950, unter der Grenze 1000 aus S6 (Spec 3.2.4).
     */
    private static final long DELIVERY_QUEUE_MAX_LENGTH = 900;

    /**
     * Der Schreibweg. "durable" heisst: die Queue überlebt einen Neustart
     * des Brokers. Was der Verbraucher endgültig ablehnt, landet über den
     * Standard-Exchange ("") in der Dead-Letter-Queue.
     */
    @Bean
    public Queue persistQueue() {
        return QueueBuilder.durable(QueueNames.PERSIST_QUEUE)
                .deadLetterExchange("")
                .deadLetterRoutingKey(QueueNames.DEAD_LETTER_QUEUE)
                .build();
    }

    /** Das Abstellgleis für Nachrichten, die niemand verarbeiten konnte. */
    @Bean
    public Queue deadLetterQueue() {
        return QueueBuilder.durable(QueueNames.DEAD_LETTER_QUEUE).build();
    }

    /**
     * Der Zustellweg. Fanout heisst: JEDE gebundene Queue bekommt eine
     * Kopie. Das ist genau richtig, weil jede web-gateway-Instanz jede
     * Nachricht sehen muss — nur sie weiss, welche ihrer WebSocket-Clients
     * im betroffenen Raum sitzen.
     */
    @Bean
    public FanoutExchange deliveryExchange() {
        return new FanoutExchange(QueueNames.DELIVERY_EXCHANGE, true, false);
    }

    /**
     * Die eigene Zustell-Queue dieser Instanz.
     *
     * Der Name wird EINMAL beim Start gebildet: Präfix, Hostname und eine
     * zufällige UUID. Die UUID macht ihn eindeutig, auch wenn zwei Instanzen
     * denselben Hostnamen haben, etwa zwei Spring-Kontexte in einem Test. Der
     * Hostname ist nur für Menschen: Docker setzt ihn auf die Container-ID, so
     * findet man in rabbitmqctl, welche Queue zu welchem Container gehört.
     *
     * Nicht durable, weil Live-Kopien nur dieser Instanz nützen; auto-delete,
     * damit sie mit der Instanz verschwindet (S4); höchstens 900 Nachrichten,
     * die älteste fällt weg, wenn die Instanz hängt (S6). Bewusst KEINE
     * Dead-Letter-Route: chat.dlq gehört dem Schreibweg (Spec 3.2.4).
     */
    @Bean
    public Queue deliveryQueue(@Value("${instance.hostname}") String hostname) {
        UUID instanceId = UUID.randomUUID();
        String queueName = QueueNames.DELIVERY_QUEUE_PREFIX + hostname + "." + instanceId;
        log.info("Delivery queue of this instance: {}", queueName);
        return QueueBuilder.nonDurable(queueName)
                .autoDelete()
                .expires(DELIVERY_QUEUE_EXPIRES_MILLIS)
                .maxLength(DELIVERY_QUEUE_MAX_LENGTH)
                .overflow(QueueBuilder.Overflow.dropHead)
                .build();
    }

    /**
     * Bindet die Zustell-Queue an den Fanout-Exchange. Erst damit bekommt diese
     * Instanz eine Kopie jeder Nachricht; ohne Bindung verwirft RabbitMQ sie
     * (Spec 1.1).
     */
    @Bean
    public Binding deliveryBinding(
            @Qualifier("deliveryQueue") Queue deliveryQueue,
            FanoutExchange deliveryExchange) {
        return BindingBuilder.bind(deliveryQueue).to(deliveryExchange);
    }

    /**
     * Nachrichten gehen als JSON über die Leitung, nicht als serialisiertes
     * Java-Objekt. Nur so kann später ein Dienst in einer anderen Sprache
     * mitlesen.
     *
     * Der ObjectMapper kommt von Spring Boot und kann bereits Instant im
     * ISO-8601-Format schreiben.
     */
    @Bean
    public MessageConverter jsonMessageConverter(ObjectMapper objectMapper) {
        return new Jackson2JsonMessageConverter(objectMapper);
    }
}
