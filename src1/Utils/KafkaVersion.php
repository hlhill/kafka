<?php
namespace EasySwoole\Kafka2\Utils;

class KafkaVersion
{
    /**
     * @var []
     */
    public static $versions;

    public static function isAtLeast($version, $compareVersion)
    {
        if (self::$versions[$version] && self::$versions[$compareVersion]) {
            for ($i = 0; $i < count(self::$versions[$version]);$i++) {
                if (self::$versions[$version][$i] > self::$versions[$compareVersion][$i]) {
                    return true;
                } else if (self::$versions[$version][$i] < self::$versions[$compareVersion][$i]) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    /**
     * @param $name
     * @param $major
     * @param $minor
     * @param $veryMinor
     * @param $patch
     */
    public static function newKafkaVersion($name, $major, $minor, $veryMinor, $patch)
    {
        self::$versions[$name] = [$major, $minor, $veryMinor, $patch];
    }
}