<?php


namespace EasySwoole\Kafka2\Config;


class NetConfig
{
    public $maxOpenRequests;

    public $connTimeout;

    public $writeTimeout;

    public $readTimeout;

    public $tls;

    /**
     * @var SaslConfig
     */
    public $sasl;

}