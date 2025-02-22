package org.petrarka.service.consumer;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.petrarka.consumer.hashtransactions.HashTransactionsConsumer;
import org.petrarka.consumer.service.KafkaConsumerService;
import org.petrarka.dto.OperationType;
import org.petrarka.dto.Transaction;
import org.petrarka.producer.service.KafkaProducerService;
import org.petrarka.producer.transaction.TransactionProducer;

import java.math.BigInteger;
import java.time.LocalDateTime;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class KafkaConsumerServiceTest {
    @Test
    void readAll() {

        sendAsyncTransactions();

        ExecutorService executorService = Executors.newFixedThreadPool(2);
        Assertions.assertDoesNotThrow(() -> CompletableFuture.runAsync(executeConsumerTransaction(), executorService));

        new KafkaConsumerService().execute(new HashTransactionsConsumer());
    }

    private Runnable executeConsumerTransaction() {
        return () -> new KafkaConsumerService().execute(new HashTransactionsConsumer());
    }


    private void sendAsyncTransactions() {
        List<Transaction> transactions = new LinkedList<>();
        transactions.add(new Transaction(
                OperationType.INVEST,
                new BigInteger("100"),
                "1234567890",
                LocalDateTime.now().toString()
        ));

        transactions.add(new Transaction(
                OperationType.DEBIT,
                new BigInteger("100"),
                "1234567890",
                LocalDateTime.now().toString()
        ));
        transactions.add(new Transaction(
                OperationType.TRANSFER,
                new BigInteger("100"),
                "1234567890",
                LocalDateTime.now().toString()
        ));

        KafkaProducerService<Transaction> kafkaProducerService = new KafkaProducerService<>();
        for (Transaction transaction : transactions) {
            kafkaProducerService.execute(new TransactionProducer(transaction));
        }

    }
}
