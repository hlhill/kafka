<?php

namespace EasySwoole\Kafka1;


use EasySwoole\Kafka1\Config\Config;
use EasySwoole\Kafka1\Exception\ConnectionException;
use EasySwoole\Kafka1\Protocol\Protocol;

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


    public function __construct()
    {

    }

    /**
     * @return mixed
     * @throws ConnectionException
     * @throws Exception\Exception
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

            $requestData = Protocol::encode(Protocol::METADATA_REQUEST, $params);
            $data = $client->send($requestData);
            $dataLen = Protocol\Protocol::unpack(Protocol\Protocol::BIT_B32, substr($data, 0, 4));
            $correlationId = Protocol\Protocol::unpack(Protocol\Protocol::BIT_B32, substr($data, 4, 4));
            // 0-4字节是包头长度
            // 4-8字节是correlationId
            $result = Protocol::decode(Protocol::METADATA_REQUEST, substr($data, 8));
            if (! isset($result['brokers'], $result['topics'])) {
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

        return $broker;
    }


}