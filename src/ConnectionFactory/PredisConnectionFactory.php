<?php

namespace RedisProxy\ConnectionFactory;

use Predis\Client;
use Redis;

class PredisConnectionFactory implements ConnectionFactory
{
    private int $optSerializer = Redis::SERIALIZER_NONE;

    public function __construct(int $optSerializer = Redis::SERIALIZER_NONE)
    {
        $this->optSerializer = $optSerializer;
    }

    /**
     * @return Client
     */
    public function create(string $host, int $port, float $timeout = 0.0)
    {
        $redis = new Client([
            'host' => $host,
            'port' => $port,
            'timeout' => $timeout,
            'serializer' => $this->optSerializer,
        ]);
        $redis->connect();
        return $redis;
    }
}
