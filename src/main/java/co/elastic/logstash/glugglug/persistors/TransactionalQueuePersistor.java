package co.elastic.logstash.glugglug.persistors;

import java.io.IOException;

/**
 * Created by andrewvc on 7/31/15.
 */
public interface TransactionalQueuePersistor {
    String getName();

    void setup() throws InternalPersistorException;

    TransactionalQueuePersistorContext newContext();
}
