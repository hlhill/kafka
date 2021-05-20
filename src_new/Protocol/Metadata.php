<?php

namespace EasySwoole\Kafka1\Protocol;

use EasySwoole\Kafka1\Exception\NotSupported;
use EasySwoole\Kafka1\Exception\Protocol as ProtocolException;

class Metadata extends Protocol
{
    /**
     * @param array $payloads
     * @return string
     * @throws ProtocolException
     * @throws NotSupported
     */
    public function encoding(array $payloads = []): string
    {
        foreach ($payloads as $topic) {
            if (! is_string($topic)) {
                throw new ProtocolException(
                    "request metadata topic array have invalid value."
                );
            }
        }

        $header = $this->requestHeader('Easyswoole-kafka', self::METADATA_REQUEST, self::METADATA_REQUEST);
        $data   = self::encodeArray($payloads, [$this, 'encodeString'], self::PACK_INT16);
        $data   = self::encodeString($header . $data, self::PACK_INT32);
        return $data;
    }

    public function decoding(string $data): array
    {
        $offset       = 0;
        $brokerRet    = $this->decodeArray(substr($data, $offset), [$this, 'metaBroker']);
        $offset      += $brokerRet['length'];
        $topicMetaRet = $this->decodeArray(substr($data, $offset), [$this, 'metaTopicMetaData']);
        $offset      += $topicMetaRet['length'];

        $result = [
            'brokers' => $brokerRet['data'],
            'topics'  => $topicMetaRet['data'],
        ];
        return $result;
    }
}