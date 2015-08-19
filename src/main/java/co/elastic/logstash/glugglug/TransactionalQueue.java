package co.elastic.logstash.glugglug;

import co.elastic.logstash.glugglug.persistors.InternalPersistorException;
import co.elastic.logstash.glugglug.persistors.TransactionalQueuePersistor;
import co.elastic.logstash.glugglug.persistors.TransactionalQueuePersistorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.SynchronousQueue;

/**
 * Created by andrewvc on 7/31/15.
 */
public class TransactionalQueue {
    private static Logger logger = LoggerFactory.getLogger(TransactionalQueue.class);
    private final String name;
    private final int size;
    private final TransactionalQueuePersistor persistor;
    private final ConcurrentHashMap<UUID,TransactionalQueueWorker> queueWorkers = new ConcurrentHashMap<UUID,TransactionalQueueWorker>();
    private final WorkDefinition workDefinition;
    private final SynchronousQueue ingestQueue = new SynchronousQueue();

    TransactionalQueue(String name_, int size_, TransactionalQueuePersistor persistor_, WorkDefinition workDefinition_)
            throws InternalPersistorException {
        name = name_;
        size = size_;
        workDefinition = workDefinition_;
        persistor = persistor_;
        logger.debug("Instantiating TransactionalQueue: " + name + "|" + size + "|" + persistor.getName());

        persistor.setup();
    }

    public void startWorkers() {
        for (int i = 0; i < size; i++) {
            TransactionalQueuePersistorContext persistorContext = persistor.newContext();
            TransactionalQueueWorker worker = new TransactionalQueueWorker(ingestQueue, persistorContext, workDefinition, 20);
            Thread thread = new Thread(worker);
            thread.setName("esqueue_wrkr|" + worker.getUUID());
            thread.start();
            queueWorkers.put(worker.getUUID(), worker);
        }
    }
}
