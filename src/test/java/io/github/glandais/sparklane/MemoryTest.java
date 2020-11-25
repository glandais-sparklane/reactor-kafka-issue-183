package io.github.glandais.sparklane;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockserver.client.MockServerClient;
import org.reactivestreams.Publisher;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.MockServerContainer;
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
import reactor.netty.http.client.HttpClient;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.awaitility.Awaitility.await;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

@Testcontainers
public class MemoryTest {

    protected static final String INPUT_TOPIC = "input";

    protected static final String OUTPUT_TOPIC = "output";

    @Container
    public KafkaContainer kafka = new KafkaContainer();

    @Container
    public MockServerContainer mockServer = new MockServerContainer();

    private KafkaSender<String, String> sender;

    AtomicInteger sentInputCounter = new AtomicInteger();
    AtomicInteger receivedInputCounter = new AtomicInteger();
    AtomicInteger mappedCounter = new AtomicInteger();
    AtomicInteger sentOutputCounter = new AtomicInteger();

    private HttpClient httpClient;

    @BeforeEach
    public void setUp() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "test-producer");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        SenderOptions<String, String> senderOptions = SenderOptions.create(props);

        sender = KafkaSender.create(senderOptions);

        new MockServerClient(mockServer.getHost(), mockServer.getServerPort())
                .when(request()
                        .withPath("/test")
                )
                .respond(response()
                        .withBody("Hello there !")
                        .withDelay(TimeUnit.MILLISECONDS, 1));

        Flux.range(0, Integer.MAX_VALUE).delayElements(Duration.ofSeconds(1)).subscribe(i -> {
            System.out.println("------------------------------------------");
            System.out.println("sentInputCounter     : " + sentInputCounter.get());
            System.out.println("receivedInputCounter : " + receivedInputCounter.get());
            System.out.println("mappedCounter        : " + mappedCounter.get());
            System.out.println("sentOutputCounter    : " + sentOutputCounter.get());
        });

        httpClient = HttpClient.create();
    }

    @Test
    public void test() {

        // send messages, indefinitely
        sendMessages(100_000);

        // read messages
        Flux<ReceiverRecord<String, String>> kafkaFlux = getMessages();

        // map messages using some async
        Flux<SenderRecord<String, String, Object>> toSend = kafkaFlux.flatMap(this::map);

        // send mapped messages
        sender.send(toSend).subscribe(sr -> sentOutputCounter.incrementAndGet());

        // await until OOM
        await().atMost(1, TimeUnit.HOURS).until(() -> sentOutputCounter.get() > 1_000_000_000);
    }

    private void sendMessages(int count) {

        sender.send(Flux.range(0, count)
                .map(i -> getSenderRecord(INPUT_TOPIC, i)))
                .doOnNext(sr -> sentInputCounter.incrementAndGet())
                .count().block();
    }

    private Flux<ReceiverRecord<String, String>> getMessages() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "test-consumer");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        ReceiverOptions<String, String> receiverOptions = ReceiverOptions.create(props);

        ReceiverOptions<String, String> options = receiverOptions.subscription(List.of(INPUT_TOPIC));
        return KafkaReceiver.create(options).receive()
                .doOnNext(rr -> receivedInputCounter.incrementAndGet());
    }

    private Publisher<SenderRecord<String, String, Object>> map(ReceiverRecord<String, String> rr) {
        if (rr.offset() % 10 == 0) {
            return Mono.empty();
        } else {
            return httpClient.get()
                    .uri(mockServer.getEndpoint() + "/test")
                    .responseContent()
                    .aggregate()
                    .asString()
                    .map(s -> getSenderRecord(OUTPUT_TOPIC, mappedCounter.incrementAndGet()));
        }
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
}
