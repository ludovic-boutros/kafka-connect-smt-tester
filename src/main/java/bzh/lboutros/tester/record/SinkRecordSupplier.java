package bzh.lboutros.tester.record;

import bzh.lboutros.tester.ConnectUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.StringConverter;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;

public class SinkRecordSupplier implements RecordSupplier<SinkRecord> {
    private final String filename;
    private Converter converter;
    private final Record record;
    private final SystemTime time = new SystemTime();

    public SinkRecordSupplier(String filename) throws IOException {
        this.filename = filename;
        record = ConnectUtils.getRecordFromFile(this.filename);
    }

    @Override
    public SinkRecord get() {
        List<Header> headerList = record.getHeaders().entrySet().stream()
                .map(e -> new ConnectHeader(e.getKey(),
                        new SchemaAndValue(Schema.STRING_SCHEMA,
                                e.getValue().getBytes(StandardCharsets.UTF_8))))
                .collect(Collectors.toList());

        try {
            SchemaAndValue connectData = preProcess();

            return new SinkRecord(record.getTopic(),
                    0,
                    null,
                    record.getKey(),
                    connectData.schema(),
                    connectData.value(),
                    0,
                    time.milliseconds(),
                    TimestampType.CREATE_TIME,
                    headerList);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public void setConverter(Converter converter) {
        this.converter = converter;
    }

    @Override
    public SchemaAndValue preProcess() throws JsonProcessingException {
        return converter.toConnectData(record.getTopic(),
                record.getValueAsString().getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public byte[] postProcess(SinkRecord record) {
        try (StringConverter stringConverter = new StringConverter()) {
            RecordHeaders headers = new RecordHeaders();
            record.headers()
                    .forEach(header ->
                            headers.add(new RecordHeader(header.key(), header.value().toString().getBytes(StandardCharsets.UTF_8))));
            return stringConverter.fromConnectData(record.topic(), headers, record.valueSchema(), record.value());
        }
    }
}
