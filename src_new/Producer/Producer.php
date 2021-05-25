<?php

namespace EasySwoole\Kafka1\Producer;

class Producer
{
    protected $client;

    protected $protocol;

    protected $ack;

    public function send(){}

    protected function convertRecordSet(array $recordSet): array
    {

    }
}