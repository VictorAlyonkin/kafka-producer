package org.petrarka.consumer.hashtransactions.deserializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Deserializer;
import org.petrarka.dto.HashTransactions;

import java.io.IOException;
import java.util.Objects;

@Slf4j
public class HashTransactionsJsonDeserializer implements Deserializer<HashTransactions> {

    private final ObjectMapper objectMapper;

    public HashTransactionsJsonDeserializer() {
        this.objectMapper = new ObjectMapper();
    }

    /**
     * Конвертация массива байт в объект
     *
     * @param topic название топика
     * @param data  массив байт
     * @return транзакция
     */
    @Override
    public HashTransactions deserialize(String topic, byte[] data) {
        if (Objects.isNull(data))
            return null;

        try {
            return objectMapper.readValue(data, HashTransactions.class);
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
    }
}
