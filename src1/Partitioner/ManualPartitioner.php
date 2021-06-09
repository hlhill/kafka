<?php


namespace EasySwoole\Kafka2\Partitioner;


use EasySwoole\Kafka2\Producer\ProducerMessage;

class ManualPartitioner implements Partitioner
{
    public function partition(ProducerMessage $message, int $numPartitions): int
    {
        return $message->partition;
    }

    public function requiresConsistency(): bool
    {
        return true;
    }
}