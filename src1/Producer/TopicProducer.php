<?php


namespace EasySwoole\Kafka2\Producer;


class TopicProducer
{
    public $topic;

    /**
     * @var Producer
     */
    public $parent;

    public $handlers;

    public $partitioner;
}