<?php


namespace EasySwoole\Kafka2\Partitioner;


interface DynamicConsistencyPartitioner
{
    public function messageRequiresConsistency(): bool;
}