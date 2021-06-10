<?php


namespace EasySwoole\Kafka2\Partitioner;


class PartitionManager
{
    public static function newRandomPartitioner(string $topic): RandomPartitioner
    {
        $partitioner = new RandomPartitioner();
        $partitioner->generator = function ($nums) {
            mt_srand(explode(' ',microtime())[0] * 100000000);
            return mt_rand(0, $nums-1);
        };
        return $partitioner;
    }

    public static function newHashPartitioner(string $topic)
    {
        $partition = new HashPartitioner();
        $partition->random = self::newRandomPartitioner($topic);
        $partition->referenceAbs = false;
        return $partition;
    }
}