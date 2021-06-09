<?php


namespace EasySwoole\Kafka2\Partitioner;


use EasySwoole\Kafka2\Producer\ProducerMessage;

interface Partitioner
{
    public function partition(ProducerMessage $message, int $numPartitions): int;

    public function requiresConsistency(): bool;
}