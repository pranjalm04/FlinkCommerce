package Deserializer;

import DAO.Transactions;
import FlinkCommerce.DataStreamJob;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.log4j.Log4j2;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import java.io.IOException;
import DAO.CurrencyExchange;
import FlinkCommerce.*;
@Log4j2
public class JSONValueDeserializationSchema<T> implements DeserializationSchema<T> {
    private final ObjectMapper objectMapper =new ObjectMapper();
    private static final Logger logger = LogManager.getLogger(DataStreamJob.class);
    private final Class<T> typeClass;

    public JSONValueDeserializationSchema(Class<T> typeClass) {
        this.typeClass = typeClass;
    }
    @Override
    public void open(InitializationContext context) throws Exception {
        DeserializationSchema.super.open(context);
    }

    @Override
    public T deserialize(byte[] bytes) throws IOException {
//        try {
//            logger.error("Error while deserializing schema");
//            return objectMapper.readValue(bytes, typeClass);
//        }
//        catch (IOException e){
//            logger.error("Error while deserializing schema");
//            return null;
//        }
        return objectMapper.readValue(bytes, typeClass);
    }


    @Override
    public boolean isEndOfStream(T event) {
        return false;
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return TypeInformation.of(typeClass);
    }

}
