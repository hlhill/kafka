<?php

namespace EasySwoole\Kafka2;

use EasySwoole\Kafka2\Config\Config;
use EasySwoole\Kafka2\Protocol\ProtocolInterface;
use EasySwoole\Kafka2\Request\MetadataRequest;

class Broker
{
    public function __construct($addr)
    {
    }

    public function Open(Config $config){}

    public function GetMetadata(MetadataRequest $request)
    {
        return $this->sendAndReceive($request);
    }

    private function sendAndReceive(ProtocolInterface $protocol)
    {

    }
}