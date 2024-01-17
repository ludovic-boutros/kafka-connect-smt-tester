package bzh.lboutros.tester.record;

import bzh.lboutros.tester.ConnectUtils;
import com.fasterxml.jackson.core.JsonProcessingException;

import java.util.Map;
import java.util.TreeMap;

public class Record {
    private TreeMap<String, String> headers;
    private String key;
    private TreeMap<?, ?> value;
    private String topic;

    public Record() {
    }

    public Record(TreeMap<String, String> headers, String key, TreeMap<?, ?> value, String topic) {
        this.headers = headers;
        this.key = key;
        this.value = value;
        this.topic = topic;
    }

    public String getTopic() {
        return topic;
    }

    public Map<?, ?> getValue() {
        return value;
    }

    public String getValueAsString() throws JsonProcessingException {
        return ConnectUtils.getResultOutputEventAsNormalizedPrettyString(value);
    }

    public Map<String, String> getHeaders() {
        return headers;
    }

    public String getKey() {
        return key;
    }

    public void setHeaders(TreeMap<String, String> headers) {
        this.headers = headers;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public void setValue(TreeMap<String, Object> value) {
        this.value = value;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }
}
