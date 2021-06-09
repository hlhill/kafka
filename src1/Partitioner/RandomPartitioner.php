<?php


namespace EasySwoole\Kafka2\Partitioner;


use EasySwoole\Kafka2\Producer\ProducerMessage;

class RandomPartitioner implements Partitioner
{
    /**
     * @var callable
     */
    public $generator;

    public function partition(ProducerMessage $message, int $numPartitions): int
    {
        return call_user_func($this->generator, $numPartitions);
    }

    public function requiresConsistency(): bool
    {
        return false;
    }
}