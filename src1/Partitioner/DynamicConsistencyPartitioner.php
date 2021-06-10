<?php


namespace EasySwoole\Kafka2\Partitioner;


use EasySwoole\Kafka2\Producer\ProducerMessage;

interface DynamicConsistencyPartitioner
{
    public function messageRequiresConsistency(ProducerMessage $message): bool;
}