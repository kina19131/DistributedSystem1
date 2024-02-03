package shared.messages;

public class SimpleKVMessage implements KVMessage {

    private String key;
    private String value;
    private StatusType status;

    public SimpleKVMessage(StatusType status, String key, String value) {
        this.status = status;
        this.key = key;
        this.value = value;
    }

    @Override
    public String getKey() {
        return key;
    }

    @Override
    public String getValue() {
        return value;
    }

    @Override
    public StatusType getStatus() {
        return status;
    }
}