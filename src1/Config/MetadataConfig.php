<?php


namespace EasySwoole\Kafka2\Config;


class MetadataConfig
{
    /**
     * @var RetryConfig
     */
    public $retry;

    public $refreshFrequency = 10 * 60;

    public $full = false;

    public $timeout = -1;
}