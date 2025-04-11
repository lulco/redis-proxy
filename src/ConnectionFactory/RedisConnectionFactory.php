<?php

namespace RedisProxy\ConnectionFactory;

use Redis;

class RedisConnectionFactory implements ConnectionFactory
{
    private int $optSerializer = 0;

    public function __construct(string $optSerializer = Serializers::NONE)
    {
        match ($optSerializer) {
            Serializers::NONE => $this->optSerializer = Redis::SERIALIZER_NONE,
            Serializers::PHP => $this->optSerializer = Redis::SERIALIZER_PHP,
            Serializers::JSON => $this->optSerializer = Redis::SERIALIZER_JSON,
            Serializers::MSGPACK => $this->optSerializer = Redis::SERIALIZER_MSGPACK,
            Serializers::IG_BINARY => $this->optSerializer = Redis::SERIALIZER_IGBINARY,
            default => $this->optSerializer = Redis::SERIALIZER_NONE
        };
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
