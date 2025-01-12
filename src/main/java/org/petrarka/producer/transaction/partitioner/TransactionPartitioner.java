package org.petrarka.producer.transaction.partitioner;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.PartitionInfo;
import org.petrarka.producer.transaction.dto.Transaction;

import java.util.List;
import java.util.Map;

@Slf4j
public class TransactionPartitioner implements Partitioner {

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {

        if (!(value instanceof Transaction)) {
            log.error("Передан неправильный объект");
            throw new InvalidRecordException("Передан неправильный объект");
        }

        List<PartitionInfo> partitionInfos = cluster.partitionsForTopic(topic);
        int numberLastPartition = partitionInfos.size() - 1;

        int numberOperationType = ((Transaction) value).getOperationType().ordinal();

        if (numberOperationType > numberLastPartition) {
            log.info("номер партиции = {}", numberLastPartition);
            return numberLastPartition;
        }

        log.info("номер партиции = {}", numberOperationType);
        return numberOperationType;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
