<?php

namespace EasySwoole\Kafka2\Config;

use EasySwoole\Kafka2\Utils\KafkaVersion;
use EasySwoole\Spl\SplBean;
use Swoole\Coroutine\Client as CoroutineClient;

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

    /**
     * @return CoroutineClient
     */
    public function getNetClient(): CoroutineClient
    {
        $client = new CoroutineClient(SWOOLE_TCP);
        $settings = [
            'open_length_check'     => 1,
            'package_length_type'   => 'N',
            'package_length_offset' => 0,
            'package_body_offset'   => 4,
            'package_max_length'    => 1024 * 1024 * 10,
        ];
        $client->set($settings);
        return $client;
    }
}