package bzh.lboutros.tester;

import bzh.lboutros.tester.record.Record;
import bzh.lboutros.tester.record.RecordSupplier;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.TransformationChain;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static bzh.lboutros.tester.ConnectUtils.*;

public class SMTPipelineTester<T extends ConnectRecord<T>> implements AutoCloseable {
    private final Logger log = org.slf4j.LoggerFactory.getLogger(SMTPipelineTester.class);
    private final TransformationChain<T> transformationChain;
    private final Converter converter;

    private final Plugins plugins;

    public SMTPipelineTester(String configPath) throws IOException {
        this("connect-plugins", configPath);
    }

    public SMTPipelineTester(String pluginPath, String configPath) throws IOException {
        plugins = new Plugins(Map.of(WorkerConfig.PLUGIN_PATH_CONFIG, pluginPath));
        plugins.compareAndSwapWithDelegatingLoader();

        Map<String, String> config = getConnectorConfigAsMap(configPath);

        transformationChain = getRecordTransformationChain(config);
        converter = getConverterFromConfig(config, plugins);
    }

    @Override
    public void close() throws Exception {
        if (converter instanceof AutoCloseable) {
            ((AutoCloseable) converter).close();
        }
    }

    private TransformationChain<T> getRecordTransformationChain(Map<String, String> props) {
        ConnectorConfig connectorConfig = new ConnectorConfig(plugins, props);

        List<Transformation<T>> transformations = connectorConfig.transformations();
        return new TransformationChain<>(transformations, null) {
            @Override
            public T apply(T record) {
                for (final Transformation<T> transformation : transformations) {
                    final T current = record;

                    log.info("Applying transformation {} to {}",
                            transformation.getClass().getName(),
                            record);
                    // execute the operation
                    record = transformation.apply(current);

                    if (record == null) break;
                }

                return record;
            }
        };
    }

    public Record transformDataFromFile(RecordSupplier<T> recordSupplier) throws IOException {
        recordSupplier.setConverter(converter);

        T result = transformationChain.apply(recordSupplier.get());
        if (result == null) {
            return null;
        }
        // TODO: Do something with the output converted bytes.
        // For now, just call the converter to make sure it doesn't throw an exception.
        byte[] bytes = recordSupplier.postProcess(result);

        TreeMap<String, String> headerMap = new TreeMap<>();
        result.headers().forEach(header -> headerMap.put(header.key(), header.value().toString()));
        return new Record(headerMap,
                result.key().toString(),
                getResultOutputEventAsMap(bytes),
                result.topic());
    }
}
