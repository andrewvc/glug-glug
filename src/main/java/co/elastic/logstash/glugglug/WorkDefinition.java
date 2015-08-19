package co.elastic.logstash.glugglug;

import java.util.List;

/**
 * Created by andrewvc on 7/31/15.
 */
public interface WorkDefinition {
    void process(TransactionalQueueWorker transactionalQueueWorker, List<WrappedQueueable> batch);
}
