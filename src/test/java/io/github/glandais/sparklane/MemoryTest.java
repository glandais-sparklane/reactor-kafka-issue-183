package io.github.glandais.sparklane;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.awaitility.Awaitility.await;

@Testcontainers
public class MemoryTest {

    protected static final String INPUT_TOPIC = "input";

    protected static final int COUNT = 100_000;

    @Container
    public KafkaContainer kafka = new KafkaContainer();

    AtomicInteger sentInputCounter = new AtomicInteger();
    AtomicInteger receivedKafkaCounter = new AtomicInteger();
    AtomicInteger receivedProcCounter = new AtomicInteger();

    @BeforeEach
    public void setUp() {
        Flux.range(0, Integer.MAX_VALUE).delayElements(Duration.ofSeconds(1)).subscribe(i -> {
            System.out.println("------------------------------------------");
            System.out.println("sentInputCounter     : " + sentInputCounter.get());
            System.out.println("receivedKafkaCounter : " + receivedKafkaCounter.get());
            System.out.println("receivedProcCounter  : " + receivedProcCounter.get());
        });
    }

    @Test
    public void test() {

        // send COUNT messages
        getSender().send(Flux.range(0, COUNT)
                .map(i -> getSenderRecord(INPUT_TOPIC, i)))
                .doOnNext(sr -> sentInputCounter.incrementAndGet())
                .count().block();

        // read messages
        readMessages()
                .doOnNext(rr -> receivedKafkaCounter.incrementAndGet())
                .flatMap(this::read)
                .subscribe(rr -> receivedProcCounter.incrementAndGet());

        // await until OOM, or not
        await().atMost(1, TimeUnit.HOURS).until(() -> receivedProcCounter.get() == COUNT);
    }

    private Publisher<ReceiverRecord<String, String>> read(ReceiverRecord<String, String> rr) {
        return Mono.just(rr).delayElement(Duration.ofMillis(100L));
    }

    private KafkaSender<String, String> getSender() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "test-producer");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        SenderOptions<String, String> senderOptions = SenderOptions.create(props);
        return KafkaSender.create(senderOptions);
    }

    private SenderRecord<String, String, Object> getSenderRecord(String topic, int i) {
        return SenderRecord.create(new ProducerRecord<>(topic, String.valueOf(i), getRandomString(5000)), null);
    }

    private String getRandomString(int size) {
        StringBuilder sb = new StringBuilder(size);
        for (int i = 0; i < size; i++) {
            sb.append(ThreadLocalRandom.current().nextInt(10));
        }
        return sb.toString();
    }

    private Flux<ReceiverRecord<String, String>> readMessages() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "test-consumer");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        ReceiverOptions<String, String> receiverOptions = ReceiverOptions.create(props);

        ReceiverOptions<String, String> options = receiverOptions.subscription(List.of(INPUT_TOPIC));
        return KafkaReceiver.create(options).receive();
    }

}
