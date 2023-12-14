package bzh.lboutros.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.commons.io.IOUtils;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.TransformationChain;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.function.Supplier;

import static org.apache.kafka.connect.runtime.ConnectorConfig.NAME_CONFIG;

public class ConnectUtils {
    private static final Logger log = LoggerFactory.getLogger(ConnectUtils.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

    public static Converter getConverterFromConfig(Map<String, String> props, Plugins plugins) {
        ConnectorConfig connectorConfig = new ConnectorConfig(plugins, props);
        return plugins.newConverter(connectorConfig, WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG, Plugins.ClassLoaderUsage.CURRENT_CLASSLOADER);
    }

    public static <T extends ConnectRecord<T>> TransformationChain<T> getRecordTransformationChain(Map<String, String> props, Plugins plugins) {
        ConnectorConfig connectorConfig = new ConnectorConfig(plugins, props);

        List<Transformation<T>> transformations = connectorConfig.transformations();
        return new TransformationChain<>(transformations, null) {
            @Override
            public T apply(T record) {
                for (final Transformation<T> transformation : transformations) {
                    final T current = record;

                    log.info("Applying transformation {} to {}",
                            transformation.getClass().getName(), record);
                    // execute the operation
                    record = transformation.apply(current);

                    if (record == null) break;
                }

                return record;
            }
        };
    }

    private static byte[] getInputEventAsBytes(String filename) throws IOException {
        return IOUtils.toByteArray(
                Objects.requireNonNull(ConnectUtils.class.getResourceAsStream("/" + filename)));
    }

    public static Map<?, ?> getJsonFileAsMap(String filename) throws IOException {
        String excpectedString = IOUtils.toString(
                Objects.requireNonNull(ConnectUtils.class.getResourceAsStream("/" + filename)),
                StandardCharsets.UTF_8);
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

    public static <T extends ConnectRecord<T>> T transformDataFromFile(Supplier<T> connectRecordSupplier,
                                                                       TransformationChain<T> transformationChain) throws IOException {


        return transformationChain.apply(connectRecordSupplier.get());
    }


    public static Map<String, String> getConnectorConfigAsMap(String filename) throws IOException {
        String excpectedString = IOUtils.toString(
                Objects.requireNonNull(ConnectUtils.class.getResourceAsStream("/" + filename)),
                StandardCharsets.UTF_8);
        JsonConnectorConfig jsonConnectorConfig = OBJECT_MAPPER.readValue(excpectedString, JsonConnectorConfig.class);
        Map<String, String> config = jsonConnectorConfig.getConfig();
        config.put(NAME_CONFIG, jsonConnectorConfig.getName());
        return config;
    }

    public static class SourceRecordSupplier implements Supplier<SourceRecord> {
        private final String filename;
        private final String topic;
        private final Converter converter;

        public SourceRecordSupplier(String topic, String filename, Converter converter) {
            this.topic = topic;
            this.filename = filename;
            this.converter = converter;
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
    }

    public static class SinkRecordSupplier implements Supplier<SinkRecord> {
        private final String filename;
        private final String topic;
        private final Converter converter;

        public SinkRecordSupplier(String topic, String filename, Converter converter) {
            this.topic = topic;
            this.filename = filename;
            this.converter = converter;
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
    }
}
