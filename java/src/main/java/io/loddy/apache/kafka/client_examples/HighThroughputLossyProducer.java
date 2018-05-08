package io.loddy.apache.kafka.client_examples;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

/*
 This example producer shows how to make a very high throughput producer that is lossy.
 The producer does a best effort to not lose messages, but ultimately could lose messages
 because of inadequate error handling.

 To measure throughput, look at the following JMX metric for each broker:
   kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec

 This metric does not include replication traffic, hence why it's an accurate throughput
 measure.

 To measure produce time, look at the following JMX metric on each broker:
   kafka.network:type=RequestMetrics,name=TotalTimeMs,request=Produce
 */
public class HighThroughputLossyProducer {
  private static final Logger log = LoggerFactory.getLogger(HighThroughputLossyProducer.class);

  public static final int MESSAGE_KEY_SIZE = 16;
  public static final int MESSAGE_VALUE_SIZE = 500;
  public static final int NUM_PRE_GENERATED_MESSAGES = 1000000;
  public static final String RANDOM_CHARS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
  public static final String TOPIC = "lossy-topic";

  private static Properties createConfig() {
    Properties props = new Properties();

    // boilerplate configuration.
    props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
            org.apache.kafka.common.serialization.StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
            org.apache.kafka.common.serialization.StringSerializer.class);

    // best-effort data loss prevention.
    props.put(ProducerConfig.RETRIES_CONFIG, 100);
    props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 100);
    props.put(ProducerConfig.ACKS_CONFIG, "all");

    // batching and compression settings. Batching introduces latency to batch messages together,
    // which amortizes RPC overhead and increases throughput. When compression is enabled with batching,
    // the entire batch is compressed, which makes the on-wire payload smaller and further improves
    // throughput.
    props.put(ProducerConfig.LINGER_MS_CONFIG, 100);
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, 1024000);
    props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
    // NOTE: start with the above batching and compression settings, but benchmark and tweak to see
    //       what values will create the most throughput.

    return props;
  }

  private static String generateRandomString(int size) {
    StringBuilder sb = new StringBuilder();
    Random rnd = new Random();
    while (sb.length() < size) {
      int index = (int) (rnd.nextFloat() * RANDOM_CHARS.length());
      sb.append(RANDOM_CHARS.charAt(index));
    }
    return sb.toString();
  }

  private static Map<String, String> generateMessages() {
    Map<String, String> messages = new HashMap<>(NUM_PRE_GENERATED_MESSAGES);
    for (int i = 0; i < NUM_PRE_GENERATED_MESSAGES; i++) {
      String key = null;
      while (key == null) {
        String keyAttempt = generateRandomString(MESSAGE_KEY_SIZE);
        if (! messages.containsKey(keyAttempt)) {
          key = keyAttempt;
        }
      }
      messages.put(key, generateRandomString(MESSAGE_VALUE_SIZE));
    }
    return messages;
  }

  public static void main(String[] args) {
    // generate a set of messages before starting the producer. Doing so will more accurately
    // simulate a real-world environment, where message throughput is not affected by the
    // random number generator. However, the strings generated here are more random than
    // a real-world example, which means the compression ratio will likely be worse.
    Map<String, String> messages = generateMessages();

    Producer<String, String> producer = new KafkaProducer<>(createConfig());

    // graceful shutdown.
    AtomicBoolean shuttingDown = new AtomicBoolean(false);
    AtomicBoolean shutdownReady = new AtomicBoolean(false);
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      log.info("Starting graceful shutdown.");
      shuttingDown.set(true);

      while (! shutdownReady.get()) {}
      producer.close();
      log.info("Graceful shutdown complete.");
    }));

    // NOTE: KafkaProducer has a single IO thread, which means the throughput this JVM process can achieve
    // will potentially be limited by the single IO thread. To achieve a higher throughput in a single
    // JVM process, create and use multiple KafkaProducer instances. Keep in mind that message ordering
    // is only guaranteed within a single KafkaProducer instance. Meaning, if you have two KafkaProducers,
    // and you produce message A to KafkaProducer A and then message B to KafkaProducer B, message B could
    // potentially be stored in the topic before message A.

    log.info("Producing messages indefinitely.");
    while (! shuttingDown.get()) {
      for (String key : messages.keySet()) {
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, key, messages.get(key));
        producer.send(record, (metadata, error) -> {
          if (error != null) {
            // this is where the producer is lossy. When a message has exceeded its retries, or
            // when the message fails to produce and isn't retry-able (eg a message that is too large),
            // this producer simply logs an error message, effectively losing the message.
            log.error("Message couldn't be produced. Message:\n%s\nError:%s", metadata, error);
          }
        });
      }
    }
    shutdownReady.set(true);
  }
}
