package kafka.tutorial3;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.util.TextUtils;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

public class ElasticSearchClientFactory {
    String hostname;
    Integer port;
    String protocol;
    String username;
    String password;


    public ElasticSearchClientFactory(String hostname, Integer port, String protocol) {
        this.hostname = hostname;
        this.port = port;
        this.protocol = protocol;
    }

    public ElasticSearchClientFactory setCredentials(String username, String password) {
        this.username = username;
        this.password = password;
        return this;
    }

    public RestHighLevelClient create() {
        boolean useCredentials = false;
        if (!TextUtils.isEmpty(username) && !TextUtils.isEmpty(password)) {
            useCredentials = true;
        }

        RestClientBuilder builder;
        if (useCredentials) {
            // if username, password are required for ES connection
            final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY,
                    new UsernamePasswordCredentials(username, password));

            builder = RestClient.builder(
                    // connnecting to hostname
                    new HttpHost(hostname, port, protocol)
            ).setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                @Override
                public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                    // use credential when connecting to host name
                    return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                }
            });
        } else {
            builder = RestClient.builder(new HttpHost(hostname, port, protocol));
        }

        return new RestHighLevelClient(builder);
    }

}
