package kafka.tutorial3;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ElasticSearchConsumer {
    public static RestHighLevelClient createClient() {
        String hostname = System.getenv("ES_HOSTNAME");
        //String username = System.getenv("ES_USERNAME");
        //String password = System.getenv("ES_PASSWORD");
        String httpScheme = System.getenv("ES_PROTOCOL");
        String portStr = System.getenv("ES_PORT");
        Integer port = Integer.parseInt(portStr);

        // don't do if you run local ES
        /*
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(username, password));
        */

        RestClientBuilder builder = RestClient.builder(
                // connnecting to hostname
                new HttpHost(hostname, port, "http")
        );

        // only for credential required
                /*
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
            @Override
            public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                // use credential when connecting to host name
                return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
            }
        });
        */

        return new RestHighLevelClient(builder);
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
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // disable autocommit offset
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100"); // max poll size for consumer.poll

        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Arrays.asList(topic));
        return consumer;
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());
        RestHighLevelClient client = createClient();

        KafkaConsumer<String, String> consumer = createConsumer("twitter_tweets");

        while (true) {
            ConsumerRecords<String, String> records
                    = consumer.poll(Duration.ofMillis(100)); // new in Kafka 2.0.0

            Integer recordCount = records.count();
            logger.info("Received " + recordCount + " records...");

            BulkRequest bulkRequest = new BulkRequest();

            for (ConsumerRecord<String, String> record: records) {
                // 2 strategies to make unique id per log
                // 1. kafka generic ID
                //String id = record.topic() + "_" + record.partition() + "_" + record.offset();

                // 2. twitter feed specific id
                String id = extractIdFromTweet(record.value());

                // where we insert data into ES
                IndexRequest indexRequest = new IndexRequest("twitter");
                indexRequest.id(id);  // to make consumer idempotent
                indexRequest.source(record.value(), XContentType.JSON);

                bulkRequest.add(indexRequest);

                //IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
                //logger.info(indexResponse.getId());
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                   e.printStackTrace();
                }
            }

            if (recordCount > 0) {
                BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);

                // If consumer stops in the middle of 'for records loop', all records in 'records' are not committed.
                // After starting consumer again, 'for loop' starts with the first record of the stopped loop.
                logger.info("Committing offsets...");
                consumer.commitSync();
                logger.info("Offsets have been committed");
            }
        }

        // close the client gracefully
        //client.close();
    }

    private static String extractIdFromTweet(String tweetJson) {
        return JsonParser.parseString(tweetJson)
                .getAsJsonObject()
                .get("id_str").getAsString();
    }
}
