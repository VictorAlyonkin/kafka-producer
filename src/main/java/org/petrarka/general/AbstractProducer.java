package org.petrarka.general;

import lombok.Data;
import org.petrarka.general.config.AbstractKafkaConfig;

@Data
public abstract class AbstractProducer<T> {

    /**
     * Объект для отправки в топик событий
     */
    protected T object;

    /**
     * Путь до JSON схемы для валидации
     */
    protected String pathToFileJsonSchema;

    /**
     * Конфигурационный класс производителя кафки
     */
    protected AbstractKafkaConfig kafkaConfig;
}
