<?php


namespace EasySwoole\Kafka2;


use EasySwoole\Kafka2\Config\Config;
use EasySwoole\Kafka2\Exception\ConfigurationException;
use EasySwoole\Kafka2\Exception\ErrorClosedClientException;
use EasySwoole\Kafka2\Exception\OutOfBrokerException;
use EasySwoole\Kafka2\Request\MetadataRequest;
use EasySwoole\Kafka2\Utils\KafkaVersion;
use Swoole\Coroutine;

class Client
{
    /**
     * @var Config
     */
    private $config;

    private $closer;

    private $closed;

    /**
     * @var Broker[]
     */
    private $seedBrokers;

    /**
     * @var Broker[]
     */
    private $deadSeeds;

    private $controllerID;

    /**
     * @var array
     */
    private $brokers;

    private $metadata;

    private $metadataTopics;

    private $coordinators;

    private $cachedPartitionsResults;

    public function __construct(Config $config)
    {
        $this->config = $config;
    }

    public static function newClient(array $addr, Config $config = null)
    {
        if (is_null($config)) {
            $config = Config::NewConfig();
        }

        $config->validate();

        if (empty($host)) {
            throw new ConfigurationException('You must provide at least one broker address');
        }

        $client = new self($config);

        $client->randomizeSeedBrokers($addr);

        if ($config->metadata->full) {
            $client->RefreshMetadata();
        }
    }

    private function randomizeSeedBrokers(array $addr)
    {
        shuffle($addr);
        foreach ($addr as $item) {
            $this->seedBrokers[] = new Broker($item);
        }
    }

    public function RefreshMetadata(...$topics)
    {
        if ($this->closed) {
            throw new ErrorClosedClientException();
        }

        foreach ($topics as $topic) {
            if ($topic == '') {
                throw new ConfigurationException('Error invalid topic');
            }
        }
        $deadline = time();
        if ($this->config->metadata->timeout > 0) {
            $deadline = $deadline + $this->config->metadata->timeout;
        }
        return $this->tryRefreshMetadata($topics, $this->config->metadata->retry->max, $deadline);
    }

    private function tryRefreshMetadata($topics, $attemptsRemaining, $deadline)
    {
        $pastDeadline = function($backoff) use ($deadline) {
            if ($deadline && time() + $backoff >= $deadline) {
                return true;
            }
            return false;
        };

        $retry = function(\Throwable $error)use($topics, $attemptsRemaining, $deadline, $pastDeadline){
            if ($attemptsRemaining > 0) {
                $backoff = $this->computeBackoff($attemptsRemaining);
                if ($pastDeadline($backoff)) {
                    return $error;
                }
                if ($backoff > 0 ) {
                    Coroutine::sleep($backoff);
                }
                return $this->tryRefreshMetadata($topics, $attemptsRemaining - 1, $deadline);
            }
            throw $error;
        };

        $broker = $this->any();

        for (;$broker != null && !$pastDeadline(0);$broker = $this->any()) {
            $allowAutoTopicCreation = true;
            if (count($topics) <= 0) {
                $allowAutoTopicCreation = false;
            }

            $request = new MetadataRequest($topics, $allowAutoTopicCreation);
            if (KafkaVersion::isAtLeast($this->config->version, V1_0_0_0)) {
                $request->version = 5;
            } else if (KafkaVersion::isAtLeast($this->config->version, V0_10_0_0)) {
                $request->version = 1;
            }
            $response = $broker->getMetadata($request);
        }

        return $retry(new OutOfBrokerException('kafka: client has run out of available brokers to talk to (Is your cluster reachable?)'));
    }

    private function computeBackoff(int $attemptsRemaining)
    {
        if (is_callable($this->config->metadata->retry->backoffFunc)) {
            $maxRetries = $this->config->metadata->retry->max;
            $retires = $maxRetries - $attemptsRemaining;
            return call_user_func($this->config->metadata->retry->backoffFunc, $retires, $maxRetries);
        }
        return $this->config->metadata->retry->max;
    }

    private function any()
    {
        if (count($this->seedBrokers) > 0) {
            $this->seedBrokers[0]->Open($this->config);
            return $this->seedBrokers[0];
        }

        foreach ($this->brokers as $broker) {
            $broker->Open($this->config);
            return $broker;
        }

        return null;
    }

    public function config()
    {
        return $this->config;
    }

    public function brokers()
    {
        return $this->brokers;
    }


}