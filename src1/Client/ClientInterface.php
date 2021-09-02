<?php

namespace EasySwoole\Kafka2\Client;

use EasySwoole\Kafka2\Config\Config;

interface ClientInterface
{
    public function config(): Config;

    public function controller();
}