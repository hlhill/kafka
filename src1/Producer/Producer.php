<?php


namespace EasySwoole\Kafka2\Producer;


use EasySwoole\Kafka2\Client;
use EasySwoole\Kafka2\Config\Config;

class Producer
{
    public static function newSyncProducer(array $address, Config $config = null)
    {
        if (is_null($config)) {
            $config = Config::NewConfig();
            $config->producer->return->success = true;
        }
        self::verifyProducerConfig($config);
        $producer = self::newAsyncProducer($address, $config);

    }

    public static function newAsyncProducer(array $address, Config $config)
    {
        $client = Client::newClient($address, $config);
    }

    private static function verifyProducerConfig(Config $config)
    {
        if (!$config->producer->return->errors) {

        }
    }
}