package co.elastic.logstash.glugglug;

/**
 * Created by andrewvc on 7/31/15.
 */
public class WrappedQueueable {
    private final long offset;
    private final Queueable queueable;
    WrappedQueueable(long offset_, Queueable queueable_) {
        offset = offset_;
        queueable = queueable_;
    }

    public long getOffset() {
        return offset;
    }

    public Queueable getQueueable() {
        return queueable;
    }
}
