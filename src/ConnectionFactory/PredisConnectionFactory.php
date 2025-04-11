<?php

namespace RedisProxy\ConnectionFactory;

use Predis\Client;

class PredisConnectionFactory implements ConnectionFactory
{
    private ?string $optSerializer = null;

    public function __construct(string $optSerializer = Serializers::NONE)
    {
        match ($optSerializer) {
            Serializers::NONE => $this->optSerializer = null,
            Serializers::PHP => $this->optSerializer = 'php',
            Serializers::JSON => $this->optSerializer = 'json',
            Serializers::MSGPACK => $this->optSerializer = 'msgpack',
            Serializers::IG_BINARY => $this->optSerializer = 'igbinary',
            default => $this->optSerializer = null
        };
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
            'serializer' => (string) $this->optSerializer,
        ]);
        $redis->connect();
        return $redis;
    }
}
