<?php

namespace EasySwoole\Kafka2\Broker;

use EasySwoole\Kafka2\Config\Config;

class Broker
{
    /**
     * @var Config
     */
    private $config;

    /**
     * @var string
     */
    private $rack;

    /**
     * @var int
     */
    private $id;

    /**
     * @var string
     */
    private $addr;

    /**
     * @var int
     */
    private $correlationID;

    /**
     * @var \swoole_client
     */
    private $conn;

    /**
     * @var int
     */
    private $opened;
}