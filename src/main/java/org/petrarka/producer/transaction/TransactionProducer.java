package org.petrarka.producer.transaction;

import lombok.Data;
import lombok.EqualsAndHashCode;
import org.petrarka.general.AbstractProducer;
import org.petrarka.producer.transaction.config.TransactionKafkaConfig;
import org.petrarka.dto.Transaction;

@Data
@EqualsAndHashCode(callSuper = true)
public class TransactionProducer extends AbstractProducer<Transaction> {

    public TransactionProducer(Transaction transaction) {
        this.object = transaction;
        this.pathToFileJsonSchema = "TransactionJsonSchema.json";
        this.kafkaConfig = new TransactionKafkaConfig();
    }
}
