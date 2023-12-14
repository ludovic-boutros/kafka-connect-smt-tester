package bzh.lboutros.tester;

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

import static bzh.lboutros.tester.ConnectUtils.getConnectorConfigAsMap;
import static bzh.lboutros.tester.ConnectUtils.getConverterFromConfig;

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
                            transformation.getClass().getName(), record);
                    // execute the operation
                    record = transformation.apply(current);

                    if (record == null) break;
                }

                return record;
            }
        };
    }

    public Result transformDataFromFile(String inputTopic, String inputFilename, ConnectUtils.RecordSupplier<T> recordSupplier) throws IOException {
        recordSupplier.setTopic(inputTopic);
        recordSupplier.setFilename(inputFilename);
        recordSupplier.setConverter(converter);

        T result = transformationChain.apply(recordSupplier.get());
        return new Result(getResultOutputEventAsNormalizedPrettyString(result), result.topic());

    }

    private String getResultOutputEventAsNormalizedPrettyString(T result) throws IOException {
        return ConnectUtils.getResultOutputEventAsNormalizedPrettyString(result, converter);
    }
}
