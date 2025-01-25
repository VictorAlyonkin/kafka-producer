package org.petrarka.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.petrarka.db.HashTransactionsDB;
import org.petrarka.dto.HashTransactions;
import org.petrarka.dto.Transaction;
import org.petrarka.producer.service.KafkaProducerService;
import org.petrarka.producer.transaction.TransactionProducer;

import java.time.LocalDateTime;
import java.util.List;

@Slf4j
@RequiredArgsConstructor
public class CheckerSendTransactions {

    /**
     * Проверка полученной хеш суммы отправленных транзакций с существующих транзакций за полученный периодом времени
     *
     * @param hashTransactions Объект хеш суммы транзакций за указанный интервал времени
     */
    public void check(HashTransactions hashTransactions) {
        LocalDateTime startDate = LocalDateTime.parse(hashTransactions.getStartDateTime());
        LocalDateTime finishDate = LocalDateTime.parse(hashTransactions.getFinishDateTime());

        List<Transaction> actualTransactions = HashTransactionsDB.transactions.stream()
                .filter(transaction ->
                        LocalDateTime.parse(transaction.getDateOperation()).isAfter(startDate)
                                && LocalDateTime.parse(transaction.getDateOperation()).isBefore(finishDate))
                .toList();

        int hashSumActualTransactions = actualTransactions.stream()
                .mapToInt(Transaction::hashCode)
                .sum();
        log.info("что пришло hash{}", hashTransactions.getHashAmount());
        log.info("что пришло hash: {}", hashSumActualTransactions);


        log.info("список транзакций на локале {}", actualTransactions);

        if (hashSumActualTransactions == hashTransactions.getHashAmount()) {
            log.info("Сверка отправленных транзакций за промежуток времени с {} до {} прошла успешно",
                    hashTransactions.getStartDateTime(),
                    hashTransactions.getFinishDateTime());
            return;
        }
        log.info("Сверка отправленных транзакций за промежуток времени с {} до {} неудачна. Выполняется повторная отправка данных",
                hashTransactions.getStartDateTime(),
                hashTransactions.getFinishDateTime());

        actualTransactions.forEach(transaction -> KafkaProducerService.execute(new TransactionProducer(transaction)));
    }
}
