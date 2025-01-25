package org.petrarka.service;

import org.junit.jupiter.api.Test;
import org.petrarka.db.HashTransactionsDB;
import org.petrarka.dto.HashTransactions;
import org.petrarka.dto.OperationType;
import org.petrarka.dto.Transaction;

import java.math.BigInteger;
import java.time.LocalDateTime;
import java.util.LinkedList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

public class CheckerSendTransactionsTest {

    @Test
    void checkTransactions_ok() {
        HashTransactionsDB.transactions.addAll(getThreeTransactions());
        assertDoesNotThrow(() -> new CheckerSendTransactions().check(getHashTransactions(getThreeTransactions())));
    }

    @Test
    void checkTransactions_differentTransactionsAndSetdToTopic() {
        HashTransactionsDB.transactions.addAll(getThreeTransactions());
        assertDoesNotThrow(() -> new CheckerSendTransactions().check(getHashTransactions(getTwoTransactions())));
    }

    private List<Transaction> getThreeTransactions() {

        List<Transaction> transactions = getTwoTransactions();

        transactions.add(new Transaction(
                OperationType.INVEST,
                new BigInteger("100"),
                "1234567890",
                LocalDateTime.now().toString()
        ));

        return transactions;
    }

    private List<Transaction> getTwoTransactions() {
        List<Transaction> transactions = new LinkedList<>();
        transactions.add(new Transaction(
                OperationType.TRANSFER,
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

        return transactions;
    }

    private HashTransactions getHashTransactions(List<Transaction> transactions) {
        int hashAmount = transactions
                .stream()
                .mapToInt(Transaction::hashCode)
                .sum();

        System.out.println("хеш код: " + hashAmount);

        return new HashTransactions()
                .setHashAmount(hashAmount)
                .setStartDateTime(LocalDateTime.now().minusSeconds(5).toString())
                .setFinishDateTime(LocalDateTime.now().plusSeconds(5).toString());
    }
}
