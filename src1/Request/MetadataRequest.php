<?php

namespace EasySwoole\Kafka2\Request;

use EasySwoole\Kafka2\Protocol\ProtocolInterface;

class MetadataRequest implements ProtocolInterface
{
    public $version;

    /**
     * @var string[]
     */
    public $topics;

    /**
     * @var bool
     */
    public $allowAutoTopicCreation;

    public function __construct($topics, $allowAutoTopicCreation, $version = null)
    {
        $this->topics = $topics;
        $this->allowAutoTopicCreation = $allowAutoTopicCreation;
        if (!is_null($version)) {
            $this->version = $version;
        }
    }

    public function encode(){}

    public function decode(){}

    public function key(){}

    public function version(){}

    public function headerVersion(){}

    public function requiredVersion(){}



}