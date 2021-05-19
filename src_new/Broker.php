<?php

namespace EasySwoole\Kafka1;

use EasySwoole\Kafka1\Config\Config;
use EasySwoole\Kafka1\Protocol\Protocol;

class Broker
{
    /**
     * @var int
     */
    private $groupBrokerId = 0;

    /**
     * @var array
     */
    private $topics = [];

    /**
     * @var array
     */
    private $brokers = [];

    /**
     * @var Config
     */
    private $config;

    /**
     * @return mixed
     */
    public function getGroupBrokerId()
    {
        return $this->groupBrokerId;
    }

    /**
     * @param mixed $groupBrokerId
     */
    public function setGroupBrokerId($groupBrokerId): void
    {
        $this->groupBrokerId = $groupBrokerId;
    }

    /**
     * @return array
     */
    public function getTopics(): array
    {
        return $this->topics;
    }

    /**
     * @return array
     */
    public function getBrokers(): array
    {
        return $this->brokers;
    }

    /**
     * @return mixed
     */
    public function getConfig()
    {
        return $this->config;
    }

    /**
     * @param mixed $config
     */
    public function setConfig($config): void
    {
        $this->config = $config;
    }

    /**
     * @param array $topics
     * @param array $brokersResult
     * @return bool
     * @throws Exception\ErrorCodeException
     */
    public function setData(array $topics, array $brokersResult): bool
    {
        $brokers = [];

        foreach ($brokersResult as $value) {
            $brokers[$value['nodeId']] = $value['host'] . ':' . $value['port'];
        }

        $changed = false;

        // brokers发送前后是否改变，并存最新的brokers
        if (serialize($this->brokers) !== serialize($brokers)) {
            $this->brokers = $brokers;

            $changed = true;
        }

        $newTopics = [];
        foreach ($topics as $topic) {
            if ((int) $topic['errorCode'] !== Protocol::NO_ERROR) {
                throw new Exception\ErrorCodeException();
                continue;
            }

            $item = [];

            foreach ($topic['partitions'] as $part) {
                $item[$part['partitionId']] = $part['leader'];
            }

            $newTopics[$topic['topicName']] = $item;
        }

        // topics 发送前后是否改变，并存最新的topics
        if (serialize($this->topics) !== serialize($newTopics)) {
            $this->topics = $newTopics;

            $changed = true;
        }
        return $changed;
    }
}