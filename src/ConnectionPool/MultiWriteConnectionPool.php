<?php

namespace RedisProxy\ConnectionPool;

use Monolog\Handler\RotatingFileHandler;
use Monolog\Logger;
use RedisProxy\Driver\Driver;
use RedisProxy\RedisProxyException;
use Throwable;

class MultiWriteConnectionPool implements ConnectionPool
{
    public const MAX_FAILS = 3;

    public const RETRY_WAIT = 1000;

    public const STRATEGY_RANDOM = 'random';

    public const STRATEGY_ROUND_ROBIN = 'round-robin';

    private const MICRO_TO_SECONDS = 1000;

    /**
     * @var array{array{host: string, port: int}} $masters
     */
    private array $masters;

    /**
     * @var array{array{host: string, port: int}} $slaves
     */
    private array $slaves;

    private int $database;

    private float $timeout;

    private Driver $driver;

    private array $mastersConnection = [];

    private array $slavesConnection = [];

    private int $failedCount = 0;

    private int $maxFails = self::MAX_FAILS;

    private int $retryWait = self::RETRY_WAIT;

    private bool $writeToReplicas = true;

    private string $strategy;

    private Logger $logger;

    /**
     * @param array{array{host: string, port: int}} $masters
     * @param array{array{host: string, port: int}} $slaves
     * @param string $strategy Implemented strategies: 'random', 'round-robin'
     */
    public function __construct(Driver $driver, array $masters, array $slaves, int $database = 0, float $timeout = 0.0, string $strategy = self::STRATEGY_RANDOM)
    {
        $this->driver = $driver;
        $this->masters = $masters;
        $this->slaves = $slaves;
        $this->database = $database;
        $this->timeout = $timeout;
        $this->strategy = $strategy;

        $this->logger = new Logger('MultiWriteConnectionPool', [new RotatingFileHandler(
            __DIR__ . '/../../logs/multi_write_connection_pool.log',
            0,
            Logger::DEBUG
        )]);
    }

    public function setRetryWait(int $retryWait): MultiWriteConnectionPool
    {
        $this->retryWait = $retryWait;
        return $this;
    }

    public function setMaxFails(int $maxFails): MultiWriteConnectionPool
    {
        $this->maxFails = $maxFails;
        return $this;
    }

    public function setWriteToReplicas(bool $writeToReplicas): MultiWriteConnectionPool
    {
        $this->writeToReplicas = $writeToReplicas;
        return $this;
    }

    /**
     * @throws RedisProxyException
     */
    public function getConnection(string $command)
    {
        if ($this->mastersConnection === []) {
            if (!$this->loadConnections()) {
                throw new RedisProxyException('Cannot load or establish connection to masters/replicas from configuration');
            }
        }

        if ($this->writeToReplicas && in_array($command, $this->getReadOnlyOperations(), true)) {
            $this->logger->debug('Selecting replica connection for read-only command', [
                'command' => $command,
            ]);
            return $this->getReplicaConnection();
        }

        $this->logger->debug('Selecting master connection for write command', [
            'command' => $command,
        ]);
        return $this->getMasterConnection();
    }

    public function resetConnection(): void
    {
        $this->reset();
    }

    /**
     * @throws RedisProxyException
     */
    private function loadConnections(): bool
    {
        $this->reset();
        // load masters
        foreach ($this->masters as $master) {
            try {
                $masterConnection = $this->driver->getConnectionFactory()->create($master['host'], $master['port'], $this->timeout);
                $this->mastersConnection[] = $masterConnection;
                $this->logger->debug('Master connection established', [
                    'host' => $master['host'],
                    'port' => $master['port'],
                ]);
            } catch (RedisProxyException $e) {
                $this->logger->debug('Failed to establish master connection', [
                    'host' => $master['host'],
                    'port' => $master['port'],
                    'error' => $e->getMessage(),
                ]);
                throw $e;
            } catch (Throwable $t) {
                $this->logger->debug('Failed to establish master connection', [
                    'host' => $master['host'],
                    'port' => $master['port'],
                    'error' => $t->getMessage(),
                ]);
                continue;
            }
        }
        // load replicas
        if ($this->writeToReplicas) {
            foreach ($this->slaves as $slave) {
                try {
                    $replicaConnection = $this->driver->getConnectionFactory()->create($slave['host'], $slave['port'], $this->timeout);
                    $this->slavesConnection[] = $replicaConnection;
                    $this->logger->debug('Replica connection established', [
                        'host' => $slave['host'],
                        'port' => $slave['port'],
                    ]);
                } catch (RedisProxyException $e) {
                    $this->logger->debug('Failed to establish replica connection', [
                        'host' => $slave['host'],
                        'port' => $slave['port'],
                        'error' => $e->getMessage(),
                    ]);
                    throw $e;
                } catch (Throwable $t) {
                    $this->logger->debug('Failed to establish replica connection', [
                        'host' => $slave['host'],
                        'port' => $slave['port'],
                        'error' => $t->getMessage(),
                    ]);
                    continue;
                }
            }
        }

        if ($this->mastersConnection === []) {
            $this->logger->error('No master connections available');
            return false;
        }

        return true;
    }

    public function handleFailed(int $attempt): bool
    {
        $this->failedCount++;
        $result = $this->loadConnections();

        if ($result === true) {
            return true;
        }

        usleep($this->retryWait * self::MICRO_TO_SECONDS);
        return $this->failedCount < $this->maxFails;
    }

    private function getMasterConnection()
    {
        switch ($this->strategy) {
            case self::STRATEGY_ROUND_ROBIN:
                $masterConnection = next($this->mastersConnection);
                if ($masterConnection === false) {
                    $masterConnection = reset($this->mastersConnection);
                }
                break;
            case self::STRATEGY_RANDOM:
                $masterConnection = $this->mastersConnection[array_rand($this->mastersConnection)];
                break;
            default:
                $masterConnection = $this->mastersConnection[array_rand($this->mastersConnection)];
                break;
        }
        $this->logger->debug('Selected master connection', [
            'host' => $masterConnection['host'],
            'port' => $masterConnection['port'],
        ]);
        if ($this->database) {
            $this->driver->connectionSelect($masterConnection, $this->database);
        }
        return $masterConnection;
    }

    /**
     * @throws RedisProxyException
     */
    private function getReplicaConnection()
    {
        if (count($this->slavesConnection) === 0) {
            return $this->getMasterConnection();
        }
        switch ($this->strategy) {
            case self::STRATEGY_ROUND_ROBIN:
                $slaveConnection = next($this->slavesConnection);
                if ($slaveConnection === false) {
                    $slaveConnection = reset($this->slavesConnection);
                }
                break;
            case self::STRATEGY_RANDOM:
                $slaveConnection = $this->slavesConnection[array_rand($this->slavesConnection)];
                break;
            default:
                $slaveConnection = $this->slavesConnection[array_rand($this->slavesConnection)];
                break;
        }
        $this->logger->debug('Selected replica connection', [
            'host' => $slaveConnection['host'],
            'port' => $slaveConnection['port'],
        ]);
        if ($this->database) {
            $this->driver->connectionSelect($slaveConnection, $this->database);
        }

        return $slaveConnection;
    }

    private function reset(): void
    {
        $this->mastersConnection = [];
        $this->slavesConnection = [];
    }

    private function getReadOnlyOperations(): array
    {
        return [
            'exists',
            'type',
            'keys',
            'scan',
            'randomkey',
            'ttl',
            'get',
            'mget',
            'substr',
            'strlen',
            'getrange',
            'getbit',
            'llen',
            'lrange',
            'lindex',
            'scard',
            'sismemer',
            'sinter',
            'sunion',
            'sdiff',
            'smembers',
            'sscan',
            'srandommember',
            'zrange',
            'zrevrange',
            'zrangebyscore',
            'zrevrangebyscore',
            'zcard',
            'zscore',
            'zcount',
            'zrank',
            'zrevrank',
            'zscan',
            'zlexcount',
            'zrangebylex',
            'zrevrangebylex',
            'hget',
            'hmget',
            'hexists',
            'hlen',
            'hkeys',
            'hvals',
            'hgetall',
            'hscan',
            'hstrlen',
            'ping',
            'auth',
            'echo',
            'quit',
            'object',
            'bitcount',
            'bitpos',
            'time',
            'pfcount',
            'geohash',
            'geopos',
            'geodist',
        ];
    }
}
