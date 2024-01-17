package bzh.lboutros.tester;

import bzh.lboutros.tester.record.Record;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.commons.io.IOUtils;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.storage.Converter;
import org.slf4j.Logger;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

import static org.apache.kafka.connect.runtime.ConnectorConfig.NAME_CONFIG;

public class ConnectUtils {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(ConnectUtils.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .enable(SerializationFeature.INDENT_OUTPUT)
            .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS);

    public static Converter getConverterFromConfig(Map<String, String> props, Plugins plugins) {
        ConnectorConfig connectorConfig = new ConnectorConfig(plugins, props);
        return plugins.newConverter(connectorConfig, WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG, Plugins.ClassLoaderUsage.CURRENT_CLASSLOADER);
    }

    public static Record getRecordFromFile(String filename) throws IOException {
        return OBJECT_MAPPER.readValue(getStringFromResourceOrFile(filename), Record.class);
    }

    public static Map<?, ?> getJsonFileAsMap(String filename) throws IOException {
        String excpectedString = getStringFromResourceOrFile(filename);
        return OBJECT_MAPPER.readValue(excpectedString, TreeMap.class);
    }

    public static String getJsonFileAsNormalizedPrettyString(String filename) throws IOException {
        return OBJECT_MAPPER.writeValueAsString(getJsonFileAsMap(filename));
    }

    public static Schema getSchema(Map<?, ?> value, SchemaBuilder innerBuilder) {
        SchemaBuilder builder;

        builder = Objects.requireNonNullElseGet(innerBuilder, () -> new SchemaBuilder(Schema.Type.STRUCT));
        value.forEach((k, v) -> {
            if (v instanceof String) {
                builder.field(k.toString(), Schema.OPTIONAL_STRING_SCHEMA);
            } else if (v instanceof Boolean) {
                builder.field(k.toString(), Schema.OPTIONAL_BOOLEAN_SCHEMA);
            } else if (v instanceof Integer) {
                builder.field(k.toString(), Schema.OPTIONAL_INT32_SCHEMA);
            } else if (v instanceof Long) {
                builder.field(k.toString(), Schema.OPTIONAL_INT64_SCHEMA);
            } else if (v instanceof Float) {
                builder.field(k.toString(), Schema.OPTIONAL_FLOAT32_SCHEMA);
            } else if (v instanceof Double) {
                builder.field(k.toString(), Schema.OPTIONAL_FLOAT64_SCHEMA);
            } else if (v instanceof Byte) {
                builder.field(k.toString(), Schema.OPTIONAL_BYTES_SCHEMA);
            } else if (v instanceof List) {
                throw new RuntimeException("List not supported yet");
                //TODO: Handle lists
            } else if (v instanceof Map) {
                SchemaBuilder innerStructBuilder = new SchemaBuilder(Schema.Type.STRUCT).optional();
                getSchema((Map<?, ?>) v, innerStructBuilder);

                builder.field(k.toString(), innerStructBuilder.schema());
            } else {
                throw new RuntimeException("Unknown type: " + v.getClass());
            }

        });

        return builder.build();
    }

    public static Object getValue(Map<?, ?> value, Struct innerStruct, Schema schema) {
        Struct struct = Objects.requireNonNullElseGet(innerStruct, () -> new Struct(schema));
        value.forEach((k, v) -> {
            if (v instanceof Map) {
                Schema innerSchema = schema.field(k.toString()).schema();
                Struct inner = new Struct(innerSchema);
                struct.put(k.toString(), inner);
                getValue((Map<?, ?>) v, inner, innerSchema);
            } else if (v instanceof List) {
                throw new RuntimeException("List not supported yet");
                //TODO: Handle lists
            } else {
                struct.put(k.toString(), v);
            }
        });
        return struct;
    }

    public static SchemaAndValue getJsonAsSchemaAndValue(Map<?, ?> value) throws IOException {
        Schema schema = getSchema(value, null);
        Object struct = getValue(value, null, schema);

        return new SchemaAndValue(schema, struct);
    }

    public static TreeMap<?, ?> getResultOutputEventAsMap(byte[] result) throws IOException {
        return OBJECT_MAPPER.readValue(new String(result), TreeMap.class);
    }

    public static String getResultRecordAsNormalizedPrettyString(Record result) throws IOException {
        return OBJECT_MAPPER.writeValueAsString(result);
    }

    public static String getResultOutputEventAsNormalizedPrettyString(Map<?, ?> value) throws JsonProcessingException {
        return OBJECT_MAPPER.writeValueAsString(value);
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


}
