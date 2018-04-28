package io.loddy.apache.kafka.client_examples;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.InterruptException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class ConsumeToDownstream {

  private static final Logger log = LoggerFactory.getLogger(ConsumeToDownstream.class);

  public static final long SHORTEST_COMMIT_INTERVAL_MS = 3000;

  private static Consumer<String, String> createConsumer() {
    Properties props = new Properties();

    // boilerplate configuration.
    props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
            org.apache.kafka.common.serialization.StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
            org.apache.kafka.common.serialization.StringDeserializer.class);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "my-test-consumer-group");

    // committing configuration.
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    return new KafkaConsumer<>(props);
  }

  public static void main(String[] args) {
    Consumer<String, String> consumer = createConsumer();
    consumer.subscribe(Collections.singletonList(RESTProducer.TOPIC));

    Runtime.getRuntime().addShutdownHook(new Thread(consumer::wakeup));

    Producer<String, String> deadLetterProducer = RESTProducer.createProducer();
    long lastCommitTimeMs = -1;
    try {
      while (true) {
        ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
        for (ConsumerRecord<String, String> record : records) {
          // first, handle a bad message or "poison pill". This example uses an arbitrary condition: an empty
          // key or value.
          if (record.key().isEmpty() || record.value().isEmpty()) {
            log.error("Received a poison pill: " + record);
            ProducerRecord<String, String> deadLetter = new ProducerRecord<>(
                    RESTProducer.TOPIC + ".errors", record.key(), record.value());
            try {
              // perform a synchronous send to make sure this message's offset isn't committed until
              // it's been acknowledged in the dead letter queue. A synchronous commit will slow down
              // this consumer's throughput but considering poison pills are not common, the slowdown
              // should be minor.
              deadLetterProducer.send(deadLetter).get(60, TimeUnit.SECONDS);
              continue;
            } catch (Exception e) {
              // raise an exception because the produce request failed many retries and to avoid losing
              // this message, its offset should not be committed.
              throw new RuntimeException("Dead letter could not be produced, exiting.", e);
            }
          }

          try {
            // in this simple example, the way a message is passed downstream is to just log it.
            // in a real-world example, the message would go somewhere, for example to a database,
            // another topic, another microservice, or any other downstream destination.
            log.info("Consumed message with offset: " + record.offset());
          } catch (Exception e) {
            // this example does not include retry logic for the scenario when the downstream system is
            // unavailable. Sending the message downstream should include retry logic to survive transient
            // errors. Most client libraries for downstream systems will have built-in retry logic. This
            // catch statement will therefor only run when all those retries have been exhausted, or if the
            // error is not retry-able.

            // Another alternative to raising an exception and exiting is to keep the consumer online,
            // continuing to retry the downstream system until it's back up. This solution is much trickier
            // because the following considerations need to be taken into account:
            //  * poll() needs to be called periodically to avoid the consumer timing out with the
            //    group coordinator, which would cause a rebalance. max.poll.interval.ms controls
            //    the poll() timeout.
            //  * pause() needs to be used so poll() can be called to trigger the heartbeat,
            //    but not return any messages.
            //  * a health check needs to be performed on the downstream system to know when resume()
            //    should be called, to cause poll() to start returning messages again.
            //  * offsets should not be committed while the consumer is waiting for the downstream system
            //    to come back online.
            //  * messages should not be inadvertently skipped because of a bug in the retry/pause/resume logic.
            throw new RuntimeException("Downstream system is down. Received the following error:", e);
          }
        }

        // commit only after enough time has gone by. Committing too frequently may overload the brokers
        // depending on throughput.
        if (System.currentTimeMillis() - lastCommitTimeMs >= SHORTEST_COMMIT_INTERVAL_MS) {
          try {
            consumer.commitSync(); // will retry unless an unrecoverable error occurs.
            lastCommitTimeMs = System.currentTimeMillis();
            log.debug("Committed offsets.");
          } catch (CommitFailedException cfe) {
            throw new RuntimeException("Commit failed with an unrecoverable error:", cfe);
          }
        }
      }
    } catch (InterruptException ie) {
      // do nothing because the consumer is closing.
    } finally {
      log.info("Shutting down the consumer gracefully.");
      consumer.close();
    }
  }
}
