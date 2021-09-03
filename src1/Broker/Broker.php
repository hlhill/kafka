<?php

namespace EasySwoole\Kafka2\Broker;

use EasySwoole\Kafka2\Config\Config;
use EasySwoole\Kafka2\Exception\Exception;

class Broker
{
    const SASLTypeOAuth = 'OAUTHBEARER';
    const SASLTypePlaintext = 'PLAIN';
    const SASLTypeSCRAMSHA256 = 'SCRAM-SHA-256';
    const SASLTypeSCRAMSHA512 = 'SCRAM-SHA-512';
    const SASLTypeGSSAPI = 'GSSAPI';
    const SASLHandshakeV0 = 0;
    const SASLHandshakeV1 = 1;
    const SASLExtKeyAuth = 'auth';

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
    private $host;

    /**
     * @var int
     */
    private $port;

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

    private $response;

    private $done;

    public function __construct(string $host, int $port)
    {
        $this->host = $host;
        $this->port = $port;
    }

    public function open(Config $config = null)
    {
        if ($this->opened) {
            throw new Exception('kafka: broker connection already initiated');
        }

        if (is_null($config)) {
            $config = Config::NewConfig();
        }

        $config->validate();

        go(function()use($config){
            $client = $config->getNetClient();
            if ($client->isConnected()) {}
        });
    }







}