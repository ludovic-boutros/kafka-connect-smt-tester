package bzh.lboutros.tester.record;

import bzh.lboutros.tester.ConnectUtils;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.Converter;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SourceRecordSupplier implements RecordSupplier<SourceRecord> {
    private final SystemTime time = new SystemTime();
    private final String filename;
    private Converter converter;
    private final Record record;

    public SourceRecordSupplier(String filename) throws IOException {
        this.filename = filename;
        record = ConnectUtils.getRecordFromFile(this.filename);
    }

    @Override
    public SourceRecord get() {
        List<Header> headerList = record.getHeaders().entrySet().stream()
                .map(e -> new ConnectHeader(e.getKey(),
                        new SchemaAndValue(Schema.STRING_SCHEMA,
                                e.getValue().getBytes(StandardCharsets.UTF_8))))
                .collect(Collectors.toList());

        // Initial json-like model conversion.
        SchemaAndValue connectData;
        try {
            connectData = preProcess();

            return new SourceRecord(Map.of(), Map.of(),
                    record.getTopic(),
                    0,
                    null,
                    record.getKey(),
                    connectData.schema(),
                    connectData.value(),
                    time.milliseconds(),
                    headerList
            );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void setConverter(Converter converter) {
        this.converter = converter;
    }

    @Override
    public SchemaAndValue preProcess() throws IOException {
        // TODO: Transform the input Json in a structured format.
        return ConnectUtils.getJsonAsSchemaAndValue(record.getValue());
    }

    @Override
    public byte[] postProcess(SourceRecord record) {
        RecordHeaders headers = new RecordHeaders();
        record.headers()
                .forEach(header ->
                        headers.add(new RecordHeader(header.key(),
                                header.value().toString().getBytes(StandardCharsets.UTF_8))));
        return converter.fromConnectData(record.topic(), headers, record.valueSchema(), record.value());
    }
}
