<?php

namespace RedisProxy\ConnectionFactory;

use Predis\Client;

class PredisConnectionFactory implements ConnectionFactory
{
    /**
     * @return Client
     */
    public function create(string $host, int $port, int $database, float $timeout = 0.0)
    {
        $redis = new Client(['host' => $host, 'port' => $port]);
        $redis->connect();
        $redis->select($database);
        return $redis;
    }
}