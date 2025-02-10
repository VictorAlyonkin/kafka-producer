package org.petrarka.general.config;

import lombok.Getter;
import lombok.Setter;

import java.util.Properties;

/**
 * Абстрактный конфигурационный класс производителя кафки
 */
@Setter
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
