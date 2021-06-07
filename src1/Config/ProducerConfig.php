<?php


namespace EasySwoole\Kafka2\Config;


class ProducerConfig
{
    /**
     * @var int
     */
    public $maxMessageBytes = 1000000;
    /**
     * @var int
     */
    public $requiredAck = 1;
    /**
     * @var int
     */
    public $timeout = 5000;

    public $compression;

    public $compressionLevel = -1000;

    public $idempotent = false;

    /**
     * @var ReturnConfig
     */
    public $return;

    public function partitioner(){}
}