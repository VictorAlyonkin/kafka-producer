package org.petrarka.producer.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.petrarka.db.HashTransactionsDB;
import org.petrarka.dto.Transaction;
import org.petrarka.general.AbstractProducer;
import org.petrarka.general.config.AbstractKafkaConfig;
import org.petrarka.utils.Validator;

@Slf4j
public class KafkaProducerService {

    /**
     * Запуск процесса отправки события в кафку
     *
     * @param kafkaProducer объект-производитель для отправки в топик
     */
    public static <P extends AbstractProducer<?>> void execute(P kafkaProducer) {
        if (kafkaProducer.getObject() instanceof Transaction) {
            HashTransactionsDB.transactions.add((Transaction) kafkaProducer.getObject());
        }
        sendAsync(kafkaProducer.getObject(), kafkaProducer.getPathToFileJsonSchema(), kafkaProducer.getKafkaConfig());
    }

    /**
     * Асинхронная отправка события в кафку
     *
     * @param object               объект для отправки в топик
     * @param pathToFileJsonSchema путь до файла json схемы
     */
    public static <O, C extends AbstractKafkaConfig> void sendAsync(O object,
                                                                    String pathToFileJsonSchema,
                                                                    C kafkaConfig) {
        log.info("Запуск процесса отправки события в топик");
        Validator.validateJsonSchema(object, pathToFileJsonSchema);
        KafkaProducer<String, O> producer = new KafkaProducer<>(kafkaConfig.getProperties());
        try {
            ProducerRecord<String, O> record = new ProducerRecord<>(kafkaConfig.getTopicName(), object);
            producer.send(record, executeAfterCompleted());
            log.info("Отправка выполнена успешно");
        } catch (Exception e) {
            producer.flush();
            log.info("Отправка события не выполнена");
            throw new RuntimeException(e);
        } finally {
            producer.close();
        }
    }

    /**
     * Метод передачи функции информирования выполнения отправки события в топик
     *
     * @return Функция информирования выполнения отправки события в топик
     */
    private static Callback executeAfterCompleted() {
        return (metadata, exception) ->
        {
            if (exception != null) {
                log.error("Exception: {}", exception.getMessage());
            } else {
                log.debug("offset = {}", metadata.offset());
                log.debug("topic = {}", metadata.topic());
                log.debug("partition = {}", metadata.partition());
            }
        };
    }
}
