<?php


namespace EasySwoole\Kafka2\Protocol;


interface PackageDecoder
{
    public function getInt();

    public function getArrayLength();

    public function getCompactArrayLength();

    public function getBool();

    public function getEmptyTaggedFieldArray();


    public function push();

    public function pop();
}