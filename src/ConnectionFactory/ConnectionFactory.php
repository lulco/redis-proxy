<?php

namespace RedisProxy\ConnectionFactory;

interface ConnectionFactory
{
    /**
     * @param float $timeout    timeout to perform the connection in seconds (default is unlimited).
     * @param float|null $operationTimeout    timeout of read / write operations in seconds.
     * @param string $connectMode    select connection method: 'connect' or 'pconnect'.
     * @return mixed    Redis or Predis\Client
     */
    public function create(string $host, int $port, float $timeout = 0.0, ?float $operationTimeout = null, string $connectMode = 'connect');
}
