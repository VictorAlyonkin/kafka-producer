package org.petrarka.service.producer;

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.security.oauthbearer.internals.secured.ValidateException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.petrarka.dto.OperationType;
import org.petrarka.dto.Transaction;
import org.petrarka.producer.service.KafkaProducerService;
import org.petrarka.producer.transaction.TransactionProducer;
import org.petrarka.producer.transaction.serializer.TransactionJsonSerializer;

import java.math.BigInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

class KafkaProducerServiceTestWithKafkaMock {

    @Test
    void test_ok() {

        MockProducer<String, Transaction> mockProducer = new MockProducer<>(true, new StringSerializer(), new TransactionJsonSerializer());
        KafkaProducerService<Transaction> kafkaProducerService = new KafkaProducerService<>(mockProducer);

        kafkaProducerService.execute(new TransactionProducer(getTransaction()));

        assertEquals(1, mockProducer.history().size());
        ProducerRecord<String, Transaction> sentRecord = mockProducer.history().get(0);
        Transaction transaction = sentRecord.value();

        assertEquals(getTransaction().getDateOperation(), transaction.getDateOperation());
        assertEquals(getTransaction().getAccount(), transaction.getAccount());
        assertEquals(getTransaction().getAmount(), transaction.getAmount());
        assertEquals(getTransaction().getOperationType(), transaction.getOperationType());
    }

    @Test
    void test_ValidateException() {

        MockProducer<String, Transaction> mockProducer = new MockProducer<>(true, new StringSerializer(), new TransactionJsonSerializer());
        KafkaProducerService<Transaction> kafkaProducerService = new KafkaProducerService<>(mockProducer);

        ValidateException validateException = Assertions.assertThrows(ValidateException.class,
                () -> kafkaProducerService.execute(new TransactionProducer(getBadTransaction())));

        assertEquals("$.amount: must have a minimum value of 0", validateException.getMessage());
        assertEquals(0, mockProducer.history().size());
    }

    private Transaction getTransaction() {
        return new Transaction(
                OperationType.DEBIT,
                new BigInteger("100"),
                "1234567890",
                "2025-02-20T16:06:39.979347700"
        );
    }

    private Transaction getBadTransaction() {
        return new Transaction(
                OperationType.DEBIT,
                new BigInteger("-100"),
                "1234567890",
                "2025-02-20T16:06:39.979347700"
        );
    }
}
