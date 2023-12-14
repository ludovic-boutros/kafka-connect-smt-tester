package bzh.lboutros.tester;

public class Result {
    private final String value;
    private final String topic;

    public Result(String value, String topic) {
        this.value = value;
        this.topic = topic;
    }

    public String getTopic() {
        return topic;
    }

    public String getValue() {
        return value;
    }
}
