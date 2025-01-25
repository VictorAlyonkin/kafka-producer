package org.petrarka.producer.general.config;

import lombok.Getter;

import java.util.Properties;

/**
 * Абстрактный конфигурационный класс производителя кафки
 */
@Getter
public abstract class AbstractKafkaConfig {

    /**
     * Список свойств кафки
     */
    protected Properties properties;

    /**
     * Название топика
     */
    protected String topicName;
}
