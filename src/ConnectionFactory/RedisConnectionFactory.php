<?php

namespace RedisProxy\ConnectionFactory;

use Redis;

class RedisConnectionFactory implements ConnectionFactory
{
    private int $optSerializer = Redis::SERIALIZER_NONE;

    public function __construct(string $optSerializer = Serializers::NONE)
    {
        switch ($optSerializer) {
            case Serializers::NONE:
                $this->optSerializer = Redis::SERIALIZER_NONE;
                break;
            case Serializers::PHP:
                $this->optSerializer = Redis::SERIALIZER_PHP;
                break;
            case Serializers::JSON:
                $this->optSerializer = Redis::SERIALIZER_JSON;
                break;
            case Serializers::MSGPACK:
                $this->optSerializer = Redis::SERIALIZER_MSGPACK;
                break;
            case Serializers::IG_BINARY:
                $this->optSerializer = Redis::SERIALIZER_IGBINARY;
                break;
            default:
                $this->optSerializer = Redis::SERIALIZER_NONE;
                break;
        }
    }

    /**
     * @return Redis
     */
    public function create(string $host, int $port, float $timeout = 0.0, ?float $operationTimeout = null, string $connectMode = 'connect')
    {
        $redis = new Redis();
        if (strtolower($connectMode) === 'pconnect') {
            $redis->pconnect($host, $port, $timeout);
        } else {
            $redis->connect($host, $port, $timeout);
        }
        $redis->setOption(Redis::OPT_SERIALIZER, $this->optSerializer);
        if ($operationTimeout !== null) {
            $redis->setOption(Redis::OPT_READ_TIMEOUT, $operationTimeout);
        }

        return $redis;
    }
}
