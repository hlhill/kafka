<?php


namespace EasySwoole\Kafka2\Config;


class SaslConfig
{
    /**
     * @var boolean
     */
    public $enable;

    public $mechanism;

    public $version;

    public $handshake;

    public $authIdentity;

    public $user;

    public $password;

    public $SCRAMAuthzID;

    public $SCRAMClientGeneratorFunc;

    public $tokenProvider;

    public $gssapi;
}