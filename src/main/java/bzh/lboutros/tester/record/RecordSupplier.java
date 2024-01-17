package bzh.lboutros.tester.record;

import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.storage.Converter;

import java.io.IOException;
import java.util.function.Supplier;

public interface RecordSupplier<T extends ConnectRecord<T>> extends Supplier<T> {

    void setConverter(Converter converter);

    SchemaAndValue preProcess() throws IOException;

    byte[] postProcess(T record);
}
