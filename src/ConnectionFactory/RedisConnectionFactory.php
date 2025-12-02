<?php

declare(strict_types=1);

namespace RedisProxy\ConnectionFactory;

use Redis;

class RedisConnectionFactory implements ConnectionFactory
{
    private int $optSerializer;

    public function __construct(string $optSerializer = Serializers::NONE)
    {
        $this->optSerializer = match ($optSerializer) {
            Serializers::PHP => Redis::SERIALIZER_PHP,
            Serializers::JSON => Redis::SERIALIZER_JSON,
            Serializers::MSGPACK => Redis::SERIALIZER_MSGPACK,
            Serializers::IG_BINARY => Redis::SERIALIZER_IGBINARY,
            default => Redis::SERIALIZER_NONE,
        };
    }

    public function create(string $host, int $port, float $timeout = 0.0): Redis
    {
        $redis = new Redis();
        $redis->connect($host, $port, $timeout);
        $redis->setOption(Redis::OPT_SERIALIZER, $this->optSerializer);

        return $redis;
    }
}
