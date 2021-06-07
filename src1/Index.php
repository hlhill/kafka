<?php


namespace EasySwoole\Kafka2;


use EasySwoole\Kafka2\Config\Config;

class Index
{
    public function index()
    {
        $config = Config::NewConfig();
        $config->producer->return->success = true;
        $config->producer->partitioner();

    }
}