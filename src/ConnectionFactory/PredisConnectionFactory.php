<?php

declare(strict_types=1);

namespace RedisProxy\ConnectionFactory;

use Predis\Client;

class PredisConnectionFactory implements ConnectionFactory
{
    private ?string $optSerializer = null;

    public function __construct(string $optSerializer = Serializers::NONE)
    {
        switch ($optSerializer) {
            case Serializers::NONE:
                $this->optSerializer = null;
                break;
            case Serializers::PHP:
                $this->optSerializer = 'php';
                break;
            case Serializers::JSON:
                $this->optSerializer = 'json';
                break;
            case Serializers::MSGPACK:
                $this->optSerializer = 'msgpack';
                break;
            case Serializers::IG_BINARY:
                $this->optSerializer = 'igbinary';
                break;
            default:
                $this->optSerializer = null;
                break;
        }
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
