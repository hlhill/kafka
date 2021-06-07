<?php


namespace EasySwoole\Kafka2\Protocol;


interface ProtocolInterface
{
    public function encode();

    public function decode();
}