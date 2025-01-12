package org.petrarka.producer.transaction.serializer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import org.petrarka.producer.transaction.dto.Transaction;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

@Slf4j
public class TransactionJsonSerializer implements Serializer<Transaction> {

    private static final String ENCODING_UTF_8 = StandardCharsets.UTF_8.name();

    private final ObjectMapper objectMapper;

    public TransactionJsonSerializer() {
        this.objectMapper = new ObjectMapper();
    }

    /**
     * Конвертация транзации в массив байт
     *
     * @param topic       Название топика
     * @param transaction Транзакция
     * @return Массив байт
     */
    @Override
    public byte[] serialize(String topic, Transaction transaction) {

        if (Objects.isNull(transaction))
            return null;

        try {
            return objectMapper.writeValueAsString(transaction).getBytes(ENCODING_UTF_8);
        } catch (UnsupportedEncodingException e) {
            String errorMessage = "Error when serializing string to byte[] due to unsupported encoding " + ENCODING_UTF_8;
            log.error(errorMessage);
            throw new SerializationException(errorMessage, e);
        } catch (JsonProcessingException e) {
            String errorMessage = "Error JSON processing";
            log.error(errorMessage);
            throw new SerializationException(errorMessage, e);
        }
    }
}
