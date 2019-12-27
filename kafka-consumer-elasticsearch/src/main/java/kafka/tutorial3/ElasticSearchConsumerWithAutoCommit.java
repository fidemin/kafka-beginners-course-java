package kafka.tutorial3;

import com.google.gson.JsonParser;
import org.apache.http.util.TextUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ElasticSearchConsumerWithAutoCommit {
    public static RestHighLevelClient createClient() {
        String hostname = System.getenv("ES_HOSTNAME");
        String portStr = System.getenv("ES_PORT");
        String protocol = System.getenv("ES_PROTOCOL");

        String username = System.getenv("ES_USERNAME");
        String password = System.getenv("ES_PASSWORD");


        hostname = !TextUtils.isEmpty(hostname)? hostname: "127.0.0.1";
        Integer port = !TextUtils.isEmpty(portStr)? Integer.parseInt(portStr): 9200;
        protocol = !TextUtils.isEmpty(protocol)? protocol: "http";

        return new ElasticSearchClientFactory(hostname, port, protocol)
                .setCredentials(username, password)
                .create();
    }

    public static KafkaConsumer<String, String> createConsumer(String topic) {
        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "kafka-demo-elasticsearch";

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // latest, none

        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Arrays.asList(topic));
        return consumer;
    }

    public static void main(String[] args) throws IOException {
        Logger logger = LoggerFactory.getLogger(ElasticSearchConsumerWithAutoCommit.class.getName());
        RestHighLevelClient client = createClient();

        KafkaConsumer<String, String> consumer = createConsumer("twitter_tweets");

        // add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("stopping application...");
            try {
                logger.info("close ES client...");
                client.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }));

        while (true) {
            ConsumerRecords<String, String> records
                    = consumer.poll(Duration.ofMillis(100)); // new in Kafka 2.0.0

            Integer recordCount = records.count();
            logger.info("Received " + recordCount + " records...");

            for (ConsumerRecord<String, String> record: records) {
                // 2 strategies to make unique id per log
                // 1. kafka genericrID
                //String id = record.topic() + "_" + record.partition() + "_" + record.offset();

                // 2. twitter feed specific id
                String id = extractIdFromTweet(record.value());

                // where we insert data into ES
                IndexRequest indexRequest = new IndexRequest("twitter");
                indexRequest.id(id);  // to make consumer idempotent
                indexRequest.source(record.value(), XContentType.JSON);

                IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
                logger.info(indexResponse.getId());
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                   e.printStackTrace();
                }
            }
        }
    }

    private static String extractIdFromTweet(String tweetJson) {
        return JsonParser.parseString(tweetJson)
                .getAsJsonObject()
                .get("id_str").getAsString();
    }
}
