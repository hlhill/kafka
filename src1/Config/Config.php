<?php

namespace EasySwoole\Kafka2\Config;

use EasySwoole\Kafka2\Utils\KafkaVersion;
use EasySwoole\Spl\SplBean;

class Config
{
    /**
     * @var ProducerConfig
     */
    public $producer;

    /**
     * @var ConsumerConfig
     */
    public $consumer;

    /**
     * @var MetadataConfig
     */
    public $metadata;

    public $clientId;

    public $rackId;

    public $channelBufferSize;

    public $version = V1_0_0_0;

    /**
     * @var NetConfig
     */
    public $net;


    public static function NewConfig()
    {
        $config = new self();

        $config->producer = new ProducerConfig();
        $config->consumer = new ConsumerConfig();

        return $config;
    }

    public function validate(){}
}