<?php

namespace EasySwoole\Kafka1;


use EasySwoole\Kafka1\Config\Config;
use EasySwoole\Kafka1\Exception\ConnectionException;
use EasySwoole\Kafka1\Exception\Exception;
use EasySwoole\Kafka1\Exception\ErrorCodeException;
use EasySwoole\Kafka1\Protocol\Metadata;
use EasySwoole\Kafka1\Protocol\Protocol;
use EasySwoole\Kafka1\Exception\Protocol as ProtocolException;

class Process
{
    /**
     * @var Config
     */
    protected $config;

    /**
     * @var Broker
     */
    protected $broker;

    /**
     * @var Protocol
     */
    protected $protocol;


    public function __construct(Config $config)
    {
        $this->setConfig($config);
        $this->setBroker(new Broker($this->config));
        $this->setProtocol(new Protocol($this->config->getBrokerVersion()));
    }

    /**
     * @return Config|mixed
     */
    protected function getConfig()
    {
        return $this->config;
    }

    /**
     * @param Config $config
     */
    public function setConfig(Config $config): void
    {
        $this->config = $config;
    }

    /**
     * @return Broker
     */
    public function getBroker()
    {
        return $this->broker;
    }

    /**
     * @param Broker $broker
     */
    public function setBroker(Broker $broker)
    {
        $this->broker = $broker;
    }

    /**
     * @return Protocol
     */
    public function getProtocol()
    {
        return $this->protocol;
    }

    /**
     * @param Protocol $protocol
     */
    public function setProtocol(Protocol $protocol)
    {
        $this->protocol = $protocol;
    }






    /**
     * @param $protocolType
     * @param array $payloads
     * @return mixed
     * @throws ProtocolException
     */
    public function encode($protocolType, array $payloads)
    {
        return $this->protocol->encode($protocolType, $payloads);
    }

    /**
     * @param $protocolType
     * @param string $data
     * @return mixed
     * @throws ProtocolException
     */
    public function decode($protocolType, string $data)
    {
        return $this->protocol->decode($protocolType, $data);
    }

    /**
     * @throws ConnectionException
     * @throws Exception
     * @throws ErrorCodeException
     * @throws ProtocolException
     * 同步broker
     */
    public function syncMeta()
    {
        $brokerList = $this->config->getMetadataBrokerList();
        $brokerHost = [];
        foreach (explode(',', $brokerList) as $key => $val) {
            if (trim($val)) {
                $brokerHost[] = trim($val);
            }
        }
        if (count($brokerHost) === 0) {
            throw new Exception('No valid broker configured');
        }

        $syncMetaFinished = false;
        shuffle($brokerHost);
        $broker = $this->getBroker();
        foreach ($brokerHost as $host) {
            try {
                $client = $broker->getMetaConnect($host);
            } catch (ConnectionException $exception) {
                // 当Kafka的一个Broker挂掉的时候，我们从其它节点同步Meta数据
                continue;
            }

            if (! $client->isConnected()) {
                continue;
            }

            $params = [];

            $requestData = $this->encode(Protocol::METADATA_REQUEST, $params);
            $data = $client->send($requestData);
            $dataLen = Protocol::unpack(Protocol::BIT_B32, substr($data, 0, 4));
            $correlationId = Protocol::unpack(Protocol::BIT_B32, substr($data, 4, 4));
            // 0-4字节是包头长度
            // 4-8字节是correlationId
            $result = $this->decode(Protocol::METADATA_REQUEST, substr($data, 8));
            if (!isset($result['brokers'], $result['topics'])) {
                throw new Exception("Get metadata is fail, brokers or topics is null.");
            }

            // 更新 topics和brokers
            if (empty($result['brokers'])) {
                continue;
            }
            $broker->setData($result['topics'], $result['brokers']);

            // 本次同步Metadata成功了
            $syncMetaFinished = true;
        }

        if ($syncMetaFinished == false) {
            throw new ConnectionException('all brokers are unreachable.');
        }

        $this->setBroker($broker);
    }


}