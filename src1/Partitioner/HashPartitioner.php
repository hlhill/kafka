<?php


namespace EasySwoole\Kafka2\Partitioner;


use EasySwoole\Kafka2\Producer\ProducerMessage;

class HashPartitioner implements Partitioner, DynamicConsistencyPartitioner
{
    /**
     * @var Partitioner
     */
    public $random;

    /**
     * @var bool
     */
    public $referenceAbs;

    public function partition(ProducerMessage $message, int $numPartitions): int
    {
        if (is_null($message->key)) {
            return $this->random->partition($message, $numPartitions);
        }

        if ($this->referenceAbs) {
            $partition = (hexdec(hash('fnv1a32',$message->key)) & 0x7fffffff) % $numPartitions;
        } else {
            $partition = hexdec(hash('fnv1a32',$message->key)) % $numPartitions;
            if ($partition < 0) {
                $partition = -$partition;
            }
        }

        return $partition;
    }

    public function requiresConsistency(): bool
    {
        return true;
    }

    public function messageRequiresConsistency(ProducerMessage $message): bool
    {
        return is_null($message->key);
    }

}