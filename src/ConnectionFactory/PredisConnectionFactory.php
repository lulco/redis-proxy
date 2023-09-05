<?php

namespace RedisProxy\ConnectionFactory;

use Predis\Client;

class PredisConnectionFactory implements ConnectionFactory
{
    /**
     * @return Client
     */
    public function create(string $host, int $port, float $timeout = 0.0)
    {
        $redis = new Client([
            'host' => $host,
            'port' => $port,
            'timeout' => $timeout,
        ]);
        $redis->connect();
        return $redis;
    }
}
