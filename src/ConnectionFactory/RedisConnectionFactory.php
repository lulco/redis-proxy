<?php

namespace RedisProxy\ConnectionFactory;

use Redis;

class RedisConnectionFactory implements ConnectionFactory
{
    /**
     * @return Redis
     */
    public function create(string $host, int $port, float $timeout = 0.0)
    {
        $redis = new Redis();
        $redis->connect($host, $port, $timeout);
        return $redis;
    }
}
