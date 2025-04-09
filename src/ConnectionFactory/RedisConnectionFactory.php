<?php

namespace RedisProxy\ConnectionFactory;

use Redis;

class RedisConnectionFactory implements ConnectionFactory
{
    private int $optSerializer = Redis::SERIALIZER_NONE;

    public function __construct(int $optSerializer = Redis::SERIALIZER_NONE)
    {
        $this->optSerializer = $optSerializer;
    }
    /**
     * @return Redis
     */
    public function create(string $host, int $port, float $timeout = 0.0)
    {
        $redis = new Redis();
        $redis->connect($host, $port, $timeout);
        $redis->setOption(Redis::OPT_SERIALIZER, $this->optSerializer);

        return $redis;
    }
}
