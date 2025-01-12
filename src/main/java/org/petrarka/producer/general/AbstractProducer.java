package org.petrarka.producer.general;

import lombok.Data;
import org.petrarka.producer.general.config.AbstractKafkaConfig;

@Data
public abstract class AbstractProducer<T> {

    /**
     * Объект для отправки в топик событий
     */
    protected T object;

    /**
     * Путь до JSON схемы для валиации
     */
    protected String pathToFileJsonSchema;

    /**
     * Конфигурационный класс производителя кафки
     */
    protected AbstractKafkaConfig kafkaConfig;
}
