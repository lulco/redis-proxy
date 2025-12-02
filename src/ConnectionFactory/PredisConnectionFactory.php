<?php

declare(strict_types=1);

namespace RedisProxy\ConnectionFactory;

use Predis\Client;

class PredisConnectionFactory implements ConnectionFactory
{
    private ?string $optSerializer;

    public function __construct(string $optSerializer = Serializers::NONE)
    {
        $this->optSerializer = match ($optSerializer) {
            Serializers::NONE => null,
            Serializers::PHP => 'php',
            Serializers::JSON => 'json',
            Serializers::MSGPACK => 'msgpack',
            Serializers::IG_BINARY => 'igbinary',
            default => null,
        };
    }

    public function create(string $host, int $port, float $timeout = 0.0): Client
    {
        $redis = new Client([
            'host' => $host,
            'port' => $port,
            'timeout' => $timeout,
            'serializer' => (string) $this->optSerializer,
        ]);
        $redis->connect();
        return $redis;
    }
}
