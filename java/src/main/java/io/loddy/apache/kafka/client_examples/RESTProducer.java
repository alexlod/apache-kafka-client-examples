package io.loddy.apache.kafka.client_examples;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Properties;
import java.util.concurrent.TimeUnit;


@Path("/")
public class RESTProducer {
  private static final Logger log = LoggerFactory.getLogger(RESTProducer.class);

  public static final int HTTP_PORT = 8080;
  public static final String TOPIC = "my-test-topic";
  // NOTE: in production, the topic's min.insync.replicas setting should be set to 2,\
  //       and its replication factor should be set to 3.

  private Server server;
  private Producer<String, String> producer;

  protected static Producer<String, String> createProducer() {
    Properties props = new Properties();

    // boilerplate configuration.
    props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
            org.apache.kafka.common.serialization.StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
            org.apache.kafka.common.serialization.StringSerializer.class);

    // no data loss configuration (note that to not lose messages you also need to handle
    // errors correctly when calling send() -- see comments below).
    props.put(ProducerConfig.RETRIES_CONFIG, 100);
    props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 100);
    props.put(ProducerConfig.ACKS_CONFIG, "all");

    // message ordering configuration.
    props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);

    return new KafkaProducer<>(props);
  }

  public RESTProducer() {
    producer = createProducer();

    // setup Jersey.
    server = new Server(HTTP_PORT);
    ResourceConfig resourceConfig = new ResourceConfig();
    resourceConfig.register(this);
    ServletContainer container = new ServletContainer(resourceConfig);
    ServletHolder holder = new ServletHolder(container);
    ServletContextHandler contextHandler = new ServletContextHandler(ServletContextHandler.SESSIONS);
    contextHandler.addServlet(holder, "/*");
    server.setHandler(contextHandler);
  }

  /*
  This example GET endpoint produces an arbitrary message to Kafka.

  It blocks until the message is acknowledged. This blocking behavior is intended
  because it allows this REST service to put back pressure on the REST clients
  when Kafka is unavailable. Without this behavior, this REST service
  would need to handle back pressure, which is non trivial. Here are some alternative
  designs to dealing with back pressure:
    * buffer non-acknowledged messages in memory
    * buffer non-acknowledged messages on disk
    * buffer non-acknowledged messages in a separate fail-over Kafka cluster.

  Each of these options has downsides. Buffering in memory means you can lose messages
  if the REST service fails. Buffering on disk offers slightly better durability than
  memory but messages could still be lost if storage is lost. Buffering to a separate
  Kafka cluster requires consumers to be aware of multiple clusters such that some
  messages are on one cluster and other messages are on the other cluster -- this is
  very non-trivial.

  WARNING: this REST service is single-threaded, which means its throughput will be quite
  bad given each request blocks until an acknowledgment is received. In a production
  environment this service should be multi-threaded to increase throughput. A multi-threaded
  design could be achieved either with a producer per thread, or with all threads sharing
  a single producer. The former option will scale better because each producer instance
  has a single IO thread. Note that send() can be passed a callback to use the
  producer in an asynchronous design. In an asynchronous design, you may also want to
  experiment with batching and compression. See the following config options:
    * linger.ms to control the maximum time to wait for a batch.
    * batch.size to control the maximum batch size.
    * compression.type to control the compression type of the entire batch.

  Batching and compression settings should be tuned based on the application's requirements.
  Reasonable, arbitrary starting points could be:
     * linger.ms=50
     * batch.size=100000
     * compression.type="snappy"
   */
  @GET
  @Produces(MediaType.TEXT_PLAIN)
  public Response index() {
    ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, "foo", "bar");
    try {
      producer.send(record).get(60, TimeUnit.SECONDS); // .get() makes send() synchronous.
    } catch (Exception e) {
      log.error("Error occurred while producing: ", e);
      return Response.status(500).entity("Error occurred: " + e.getMessage()).build();
    }

    return Response.status(200).entity("Success").build();
  }

  public void start() {
    try {
      server.start();
    } catch (Exception e) {
      throw new RuntimeException("Couldn't start Jetty server: ", e);
    }
  }

  public void stop() {
    log.info("Initiating graceful shutdown.");
    if (server != null) {
      try {
        server.stop();
        log.info("Jetty server shutdown.");
      } catch (Exception e) {
        log.error("Couldn't stop Jetty server: ", e);
      }
    }

    if (producer != null) {
      producer.close();
    }

    log.info("Producer shutdown");
  }

  public static void main(String[] args) {
    RESTProducer server = new RESTProducer();

    Runtime.getRuntime().addShutdownHook(new Thread(server::stop));

    server.start();
  }
}
