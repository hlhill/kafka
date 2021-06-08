<?php

namespace EasySwoole\Kafka2;

use EasySwoole\Kafka2\Config\Config;
use EasySwoole\Kafka2\Exception\ConfigurationException;
use EasySwoole\Kafka2\Exception\ErrorAlreadyConnectedException;
use EasySwoole\Kafka2\Exception\ErrorNotConnectedException;
use EasySwoole\Kafka2\Protocol\ProtocolInterface;
use EasySwoole\Kafka2\Request\MetadataRequest;
use EasySwoole\Kafka2\Utils\KafkaVersion;
use Swoole\Coroutine\Client;

class Broker
{
    /**
     * @var Config
     */
    private $config;

    /**
     * @var string
     */
    private $rack;

    private $id;

    private $host;

    private $port;

    private $correlationID;

    /**
     * @var Client
     */
    private $conn;

    private $connErr;

    private $opened;

    public function __construct($addr)
    {
        $this->id = -1;
        [$host, $port] = explode(':', $addr);
        $this->host = $host;
        $this->port= $port;
    }

    public function open(Config $config = null)
    {
        if ($this->opened == 1) {
            throw new ErrorAlreadyConnectedException();
        }
        $this->opened = 1;
        if (is_null($config)) {
            $config = Config::NewConfig();
        }

        $config->validate();
        $settings = [
            'open_length_check'     => 1,
            'package_length_type'   => 'N',
            'package_length_offset' => 0,
            'package_body_offset'   => 4,
            'package_max_length'    => 1024 * 1024 * 10,
        ];

        if (!$this->conn instanceof Client) {
            $client = New Client(SWOOLE_TCP);
            $client->set($settings);

            $this->conn = $client;
        }

        if ($config->net->sasl->enable) {

        }

        $this->config = $config;
    }

    public function connected()
    {
        return is_null($this->conn);
    }

    public function close(){}

    public function getMetadata(MetadataRequest $request)
    {
        return $this->sendAndReceive($request);
    }



    private function sendAndReceive(ProtocolInterface $protocol)
    {

    }

    private function send(ProtocolInterface $request)
    {
        if (is_null($this->conn)) {
            if (!is_null($this->connErr)) {
                throw $this->connErr;
            }
            throw new ErrorNotConnectedException();
        }

        if (!KafkaVersion::isAtLeast($this->config->version, $request->requiredVersion())) {
            throw new ConfigurationException('unsupported version');
        }

        $this->conn->send($request);
    }
}