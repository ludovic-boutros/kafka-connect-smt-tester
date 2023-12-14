package bzh.lboutros.tester;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.commons.io.IOUtils;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.Converter;
import org.slf4j.Logger;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Supplier;

import static org.apache.kafka.connect.runtime.ConnectorConfig.NAME_CONFIG;

public class ConnectUtils {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(ConnectUtils.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

    public static Converter getConverterFromConfig(Map<String, String> props, Plugins plugins) {
        ConnectorConfig connectorConfig = new ConnectorConfig(plugins, props);
        return plugins.newConverter(connectorConfig, WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG, Plugins.ClassLoaderUsage.CURRENT_CLASSLOADER);
    }


    private static byte[] getInputEventAsBytes(String filename) throws IOException {
        return getStringFromResourceOrFile(filename).getBytes(StandardCharsets.UTF_8);
    }

    public static Map<?, ?> getJsonFileAsMap(String filename) throws IOException {
        String excpectedString = getStringFromResourceOrFile(filename);
        return OBJECT_MAPPER.readValue(excpectedString, TreeMap.class);
    }

    public static String getJsonFileAsNormalizedPrettyString(String filename) throws IOException {
        return OBJECT_MAPPER.writeValueAsString(getJsonFileAsMap(filename));
    }

    public static Map<?, ?> getResultOutputEventAsMap(ConnectRecord<?> result, Converter converter) throws IOException {
        String resultString = new String(converter.fromConnectData(
                result.topic(),
                result.valueSchema(),
                result.value()),
                StandardCharsets.UTF_8);
        return OBJECT_MAPPER.readValue(resultString, TreeMap.class);
    }

    public static String getResultOutputEventAsNormalizedPrettyString(ConnectRecord<?> result, Converter converter) throws IOException {
        return OBJECT_MAPPER.writeValueAsString(getResultOutputEventAsMap(result, converter));
    }


    public static Map<String, String> getConnectorConfigAsMap(String filename) throws IOException {
        String excpectedString = getStringFromResourceOrFile(filename);

        JsonConnectorConfig jsonConnectorConfig = OBJECT_MAPPER.readValue(excpectedString, JsonConnectorConfig.class);
        Map<String, String> config = jsonConnectorConfig.getConfig();
        config.put(NAME_CONFIG, jsonConnectorConfig.getName());
        return config;
    }

    private static String getStringFromResourceOrFile(String filename) throws IOException {
        String excpectedString = null;
        try (InputStream stream = ConnectUtils.class.getResourceAsStream("/" + filename)) {
            if (stream != null) {
                excpectedString = IOUtils.toString(stream, StandardCharsets.UTF_8);
            }
        }
        if (excpectedString == null) {
            log.info("Cannot load classpath file, trying with a standard file.");
            excpectedString = IOUtils.toString(new FileInputStream(filename), StandardCharsets.UTF_8);
        }
        return excpectedString;
    }

    public static class SourceRecordSupplier implements RecordSupplier<SourceRecord> {
        private String filename;
        private String topic;
        private Converter converter;

        public SourceRecordSupplier() {
        }

        @Override
        public SourceRecord get() {
            try {
                byte[] input = getInputEventAsBytes(filename);

                SchemaAndValue connectData = converter.toConnectData(topic, input);
                return new SourceRecord(Map.of(), Map.of(),
                        topic,
                        0,
                        null,
                        null,
                        connectData.schema(),
                        connectData.value());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public void setFilename(String filename) {
            this.filename = filename;
        }

        public void setTopic(String topic) {
            this.topic = topic;
        }

        public void setConverter(Converter converter) {
            this.converter = converter;
        }
    }

    public interface RecordSupplier<T extends ConnectRecord<T>> extends Supplier<T> {
        void setFilename(String filename);

        void setTopic(String topic);

        void setConverter(Converter converter);
    }

    public static class SinkRecordSupplier implements RecordSupplier<SinkRecord> {
        private String filename;
        private String topic;
        private Converter converter;

        public SinkRecordSupplier() {
        }

        @Override
        public SinkRecord get() {
            try {
                byte[] input = getInputEventAsBytes(filename);

                SchemaAndValue connectData = converter.toConnectData(topic, input);
                return new SinkRecord(topic,
                        0,
                        null,
                        null,
                        connectData.schema(),
                        connectData.value(),
                        0);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public void setFilename(String filename) {
            this.filename = filename;
        }

        public void setTopic(String topic) {
            this.topic = topic;
        }

        public void setConverter(Converter converter) {
            this.converter = converter;
        }
    }
}
