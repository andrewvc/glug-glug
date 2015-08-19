package co.elastic.logstash.glugglug.persistors;

import co.elastic.logstash.glugglug.Queueable;
import co.elastic.logstash.glugglug.TransactionalQueue;
import co.elastic.logstash.glugglug.WrappedQueueable;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Index;
import io.searchbox.indices.template.PutTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;

/**
 * Created by andrewvc on 7/31/15.
 */
public class ElasticsearchPersistor implements TransactionalQueuePersistor {
    public static Logger logger = LoggerFactory.getLogger(TransactionalQueue.class);
    final String name = "elasticsearchPersistor";
    final String url;
    final TransactionalQueuePersistorContext mainContext;
    final JestClient jestClient;
    final SynchronousQueue<Queueable> ingressQueue = new SynchronousQueue<Queueable>();

    ElasticsearchPersistor(String url_) {
        url = url_;

        // Setup client factory, to be used by contexts
        JestClientFactory jestClientFactory = new JestClientFactory();
        HttpClientConfig clientConfig = new HttpClientConfig.
                Builder(url).
                multiThreaded(true).
                maxTotalConnection(100). // NO LIMITS HERE!!!
                defaultMaxTotalConnectionPerRoute(100).
                build();
        ;
        jestClientFactory.setHttpClientConfig(clientConfig);
        jestClient = jestClientFactory.getObject();

        mainContext = new ElasticSearchPersistorContext(this);
    }

    public JestClient getJestClient() {
        return jestClient;
    }

    public String getUrl() {
        return url;
    }

    public String getName() {
        return name;
    }

    public void setup() throws InternalPersistorException {
        String template = "{\"name\":\"esqueue-segments\",\"body\":{\"template\":\".esqueue-j-*\",\"settings\":{\"index.merge.policy.segments_per_tier\":100000},\"mappings\":{\"event\":{\"properties\":{\"event\":{\"type\":\"binary\"}}}}}}";
        PutTemplate putTemplate = new PutTemplate.Builder("logstash-esqueue", template).build();
        try {
            jestClient.execute(putTemplate);
        } catch (IOException e) {
            throw new InternalPersistorException("Could not create template", e);
        }
    }

    public TransactionalQueuePersistorContext newContext() {
        return new ElasticSearchPersistorContext(this);
    }

    public void createContext() {

    }

    class ElasticSearchPersistorContext implements TransactionalQueuePersistorContext {
        final ElasticsearchPersistor esPersistor;
        final JestClient client;

        ElasticSearchPersistorContext(ElasticsearchPersistor esPersistor_) {
            esPersistor = esPersistor_;
            client = esPersistor.getJestClient();
        }

        public void writeOffset(long offset) throws InternalPersistorException {
            Map<String, Long> source = new LinkedHashMap<String, Long>();
            source.put("offset", offset);
            Index index = new Index.Builder(source).index(".esqueue-offsets").type("offset").build();
            try {
                client.execute(index);
            } catch (IOException e) {
                throw new InternalPersistorException("Could not persist offset!!!", e);
            }
        }

    public Future<Boolean> writeJournal(List<WrappedQueueable> batch) {
        Map<String,String> source = new LinkedHashMap<String, String>();

        Iterator<WrappedQueueable> batchIt = batch.iterator();
        while (batchIt.hasNext()) {
            WrappedQueueable wrappedQueueable = batchIt.next();
        }

        return null;
    }
}
}
