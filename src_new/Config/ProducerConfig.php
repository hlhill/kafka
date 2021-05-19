<?php


namespace EasySwoole\Kafka1\Config;


use EasySwoole\Kafka1\Protocol\Protocol;
use EasySwoole\Kafka1\Exception;

class ProducerConfig extends Config
{
    private const COMPRESSION_OPTIONS = [
        Protocol::COMPRESSION_NONE,
        Protocol::COMPRESSION_GZIP,
        Protocol::COMPRESSION_SNAPPY,
    ];

    private $requiredAck = 1;
    private $timeout = 5000;
    private $requestTimeout = 6000;
    private $produceInterval = 100;
    private $compression = Protocol::COMPRESSION_NONE;

    /**
     * @param int $requestTimeout
     * @throws Exception\Config
     */
    public function setRequestTimeout(int $requestTimeout): void
    {
        if ($requestTimeout < 1 || $requestTimeout > 900000) {
            throw new Exception\Config("Set Request timeout value is invalid, must set it 1 .. 900000.");
        }

        $this->requestTimeout = $requestTimeout;
    }

    public function getRequestTimeout()
    {
        return $this->requestTimeout;
    }

    /**
     * @param int $produceInterval
     * @throws Exception\Config
     */
    public function setProduceInterval(int $produceInterval): void
    {
        if ($produceInterval < 1 || $produceInterval > 900000) {
            throw new Exception\Config("Set produce interval timeout value is invalid, must set it 1.. 900000.");
        }

        $this->produceInterval = $produceInterval;
    }

    public function getProduceInterval()
    {
        return $this->produceInterval;
    }

    /**
     * @param int $timeout
     * @throws Exception\Config
     */
    public function setTimeout(int $timeout): void
    {
        if ($timeout < 1 || $timeout > 900000) {
            throw new Exception\Config("Set timeout is invalid, mudt set it 1 .. 900000.");
        }

        $this->timeout = $timeout;
    }

    public function getTimeout()
    {
        return $this->timeout;
    }

    /**
     * @param int $requiredAck
     * @throws Exception\Config
     */
    public function setRequiredAck(int $requiredAck): void
    {
        if ($requiredAck < -1 || $requiredAck > 900000) {
            throw new Exception\Config("Set required ack value is invalid, must set it -1 .. 10000.");
        }

        $this->requiredAck = $requiredAck;
    }

    public function getRequiredAck()
    {
        return $this->requiredAck;
    }

    /**
     * @param int $compression
     * @throws Exception\Config
     */
    public function setCompression(int $compression): void
    {
        if (! in_array($compression, self::COMPRESSION_OPTIONS, true)) {
            throw new Exception\Config(
                "Compression must be one the EasySwoole\Kafka\Protocol::COMPRESSION_OPTIONS_* constants."
            );
        }

        $this->compression = $compression;
    }

    public function getCompression()
    {
        return $this->compression;
    }

}