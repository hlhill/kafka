<?php


namespace EasySwoole\Kafka2\Protocol;


interface PackageEncoder
{
    public function putInt(int $in);

    public function putCompactArrayLength(int $in);

    public function putArrayLength(int $in);

    public function putPool(bool $in);


}