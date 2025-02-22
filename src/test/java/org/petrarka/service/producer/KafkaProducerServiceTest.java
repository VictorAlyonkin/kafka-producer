package org.petrarka.service.producer;

import org.apache.kafka.common.security.oauthbearer.internals.secured.ValidateException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.petrarka.dto.OperationType;
import org.petrarka.dto.Transaction;
import org.petrarka.producer.service.KafkaProducerService;
import org.petrarka.producer.transaction.TransactionProducer;

import java.math.BigInteger;
import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class KafkaProducerServiceTest {

    @Test
    void sendAsyncInvest() {
        Transaction transaction = new Transaction(
                OperationType.INVEST,
                new BigInteger("100"),
                "1234567890",
                LocalDateTime.now().toString()
        );
        KafkaProducerService<Object> kafkaProducerService = new KafkaProducerService<>();
        Assertions.assertDoesNotThrow(() -> kafkaProducerService.execute(new TransactionProducer(transaction)));
    }

    @Test
    void sendAsyncDebit() {
        Transaction transaction = new Transaction(
                OperationType.DEBIT,
                new BigInteger("100"),
                "1234567890",
                LocalDateTime.now().toString()
        );
        KafkaProducerService<Object> kafkaProducerService = new KafkaProducerService<>();
        Assertions.assertDoesNotThrow(() -> kafkaProducerService.execute(new TransactionProducer(transaction)));
    }

    @Test
    void sendAsyncTransfer() {
        Transaction transaction = new Transaction(
                OperationType.TRANSFER,
                new BigInteger("100"),
                "1234567890",
                LocalDateTime.now().toString()
        );
        KafkaProducerService<Object> kafkaProducerService = new KafkaProducerService<>();
        Assertions.assertDoesNotThrow(() -> kafkaProducerService.execute(new TransactionProducer(transaction)));
    }

    @Test
    void sendAsync_ErrorJsonValidation_AmountLessZero() {
        Transaction transaction = new Transaction(
                OperationType.INVEST,
                new BigInteger("-100"),
                "1234567890",
                LocalDateTime.now().toString()
        );

        KafkaProducerService<Object> kafkaProducerService = new KafkaProducerService<>();
        ValidateException validateException = Assertions.assertThrows(ValidateException.class,
                () -> kafkaProducerService.execute(new TransactionProducer(transaction)));

        Assertions.assertAll(
                () -> assertEquals("$.amount: must have a minimum value of 0", validateException.getMessage())
        );
    }
}
