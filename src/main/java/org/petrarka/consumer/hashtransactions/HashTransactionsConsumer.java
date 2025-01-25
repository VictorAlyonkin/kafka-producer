package org.petrarka.consumer.hashtransactions;

import lombok.experimental.Accessors;
import org.petrarka.consumer.hashtransactions.config.HashTransactionsKafkaConfig;
import org.petrarka.general.AbstractConsumer;

/**
 * Потребитель отправки событий объектов хеш суммы
 */
@Accessors(chain = true)
public class HashTransactionsConsumer extends AbstractConsumer {

    public HashTransactionsConsumer() {
        this.kafkaConfig = new HashTransactionsKafkaConfig();
    }
}