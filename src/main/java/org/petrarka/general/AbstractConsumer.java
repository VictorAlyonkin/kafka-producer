package org.petrarka.general;

import lombok.Data;
import org.petrarka.general.config.AbstractKafkaConfig;

@Data
public abstract class AbstractConsumer{

    /**
     * Конфигурационный класс потребителя кафки
     */
    protected AbstractKafkaConfig kafkaConfig;
}
