package bzh.lboutros.utils;

import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.runtime.TransformationChain;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.storage.Converter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.function.Supplier;

import static bzh.lboutros.utils.ConnectUtils.*;

public abstract class SMTPipelineTestBase<T extends ConnectRecord<T>> {
    private TransformationChain<T> transformationChain;
    private Converter converter;

    private final Plugins plugins;

    protected SMTPipelineTestBase() {
        plugins = new Plugins(Map.of(
                WorkerConfig.PLUGIN_PATH_CONFIG, "connect-plugins"));
        plugins.compareAndSwapWithDelegatingLoader();
    }

    protected abstract String getConnectorConfigurationFilename();

    @BeforeEach
    public void beforeEach() throws IOException {
        Map<String, String> config = getConnectorConfigAsMap(getConnectorConfigurationFilename());
        transformationChain = getRecordTransformationChain(config, plugins);
        converter = getConverterFromConfig(config, plugins);
    }

    @AfterEach
    public void teardown() throws IOException {
        if (converter instanceof Closeable) {
            ((Closeable) converter).close();
        }
    }


    protected T transformDataFromFile(Supplier<T> connectRecordSupplier) throws IOException {
        return ConnectUtils.transformDataFromFile(connectRecordSupplier, transformationChain);
    }

    protected String getResultOutputEventAsNormalizedPrettyString(ConnectRecord<T> result) throws IOException {
        return ConnectUtils.getResultOutputEventAsNormalizedPrettyString(result, converter);
    }

    protected Converter getConverter() {
        return converter;
    }
}
