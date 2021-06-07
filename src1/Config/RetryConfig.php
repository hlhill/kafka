<?php


namespace EasySwoole\Kafka2\Config;


class RetryConfig
{
    public $max = 3;

    public $backoff = 0;

    public $backoffFunc;
}