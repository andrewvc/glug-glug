package co.elastic.logstash.glugglug.persistors;

import co.elastic.logstash.glugglug.WrappedQueueable;

import java.util.List;
import java.util.concurrent.Future;

/**
 * Created by andrewvc on 7/31/15.
 */
public interface TransactionalQueuePersistorContext {
    public void writeOffset(long offset) throws InternalPersistorException;

    Future<Boolean> writeJournal(List<WrappedQueueable> batch);
}
