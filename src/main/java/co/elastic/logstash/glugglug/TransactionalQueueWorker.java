package co.elastic.logstash.glugglug;


import co.elastic.logstash.glugglug.persistors.InternalPersistorException;
import co.elastic.logstash.glugglug.persistors.TransactionalQueuePersistorContext;
import com.sun.istack.internal.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

/**
 * Created by andrewvc on 7/31/15.
 */
public class TransactionalQueueWorker implements Runnable{
    private static Logger logger = LoggerFactory.getLogger(TransactionalQueue.class);
    private final TransactionalQueuePersistorContext persistorContext;
    private UUID uuid;
    private long offset = 0;
    private long eventId = 0;
    private WorkDefinition workDefinition;
    private int batchSize;
    SynchronousQueue<Queueable> ingestQueue;
    SynchronousQueue<AbstractMap.SimpleImmutableEntry<SynchronousQueue, List<Queueable>>> journalQueue;

    TransactionalQueueWorker(SynchronousQueue ingestQueue_, TransactionalQueuePersistorContext persistorContext_, WorkDefinition workDefinition_, int batchSize_) {
        uuid = UUID.randomUUID();
        workDefinition = workDefinition_;
        batchSize = batchSize_;
        ingestQueue = ingestQueue_;
        persistorContext = persistorContext_;
        journalQueue = new SynchronousQueue<AbstractMap.SimpleImmutableEntry<SynchronousQueue, List<Queueable>>>();
    }

    public void run() {
        while (true) {
            List<WrappedQueueable> batch = takeBatch();
            Future<Boolean> journalBatchFuture = journalBatch(batch);
            processBatch(batch);
        }
    }

    private Future<Boolean> journalBatch(List<WrappedQueueable> batch) {
        Map<String,Byte[]>  = new LinkedHashMap<String, Byte[]>();
        return persistorContext.writeJournal()
    }

    private void processBatch(List<WrappedQueueable> batch) {
        workDefinition.process(this, batch);
        WrappedQueueable lastQueueable = batch.get(batch.size() - 1);
        writeOffset(lastQueueable.getOffset());
    }

    private void writeOffset(long offset) {
        try {
            persistorContext.writeOffset(offset);
        } catch (InternalPersistorException e) {
            logger.error("Could not persist offset in worker " + uuid, e);
        }
    }

    @NotNull
    private List<WrappedQueueable> takeBatch() {
        List<WrappedQueueable> wrappedQueueables = new ArrayList<WrappedQueueable>(20);

        int processed = 0;
        try {
            // Block till one arrives
            wrappedQueueables.add(wrapQueueable(ingestQueue.take()));
            // Try and receive more, but if we wait longer than 10ms, work with what we've got
            // TODO: Make the poll timeout configurable
            for (int i=1; i<batchSize; i++) { // start at 1 since we already took one
                Queueable queueable = ingestQueue.poll(10, TimeUnit.MILLISECONDS);
                if (queueable == null) {
                    break;
                }
                wrappedQueueables.add(wrapQueueable(queueable));
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return wrappedQueueables;
    }

    private WrappedQueueable wrapQueueable(Queueable queueable) {
        return new WrappedQueueable(offset++, queueable);
    }

    public UUID getUUID() {
        return uuid;
    }
}
