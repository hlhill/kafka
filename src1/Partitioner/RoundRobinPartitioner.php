<?php


namespace EasySwoole\Kafka2\Partitioner;


use EasySwoole\Kafka2\Producer\ProducerMessage;

class RoundRobinPartitioner implements Partitioner
{
    public $partition;

    public function partition(ProducerMessage $message, int $numPartitions): int
    {
        if ($this->partition >= $numPartitions) {
            $this->partition = 0;
        }
        $p = $this->partition;
        $this->partition++;
        return $p;
    }

    public function requiresConsistency(): bool
    {
        return false;
    }
}