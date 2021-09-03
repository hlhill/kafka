<?php


namespace EasySwoole\Kafka2\Config;


class NetConfig
{
    public $maxOpenRequests;

    public $connTimeout;

    public $writeTimeout;

    public $readTimeout;

    /**
     * @var
     */
    public $tls;

    /**
     * @var SaslConfig
     */
    public $sasl;

    /**
     * @var ProxyConfig
     */
    public $proxy;

}