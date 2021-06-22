<?php


namespace EasySwoole\Kafka2\Producer;


use EasySwoole\Kafka2\Partitioner\Partitioner;

class TopicProducer
{
    public $topic;

    /**
     * @var Producer
     */
    public $parent;

    public $handlers;

    public $breaker;

    /**
     * @var Partitioner
     */
    public $partitioner;

    public function partitionMessage(ProducerMessage $message)
    {
        $partitions = [];

        try {

        }catch (\Throwable $throwable) {

        }
    }

}