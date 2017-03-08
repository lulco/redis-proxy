<?php

namespace RedisProxy;

use Predis\Client;
use Predis\Response\Status;
use Redis;

trait CommonBehavior
{
    use AbstractCommonBehavior;

    protected function init()
    {
        $this->prepareDriver();
        $this->select($this->database);
    }

    /**
     * @return string|null
     */
    public function actualDriver()
    {
        if ($this->driver instanceof Redis) {
            return self::DRIVER_REDIS;
        }
        if ($this->driver instanceof Client) {
            return self::DRIVER_PREDIS;
        }
        return null;
    }

    protected function prepareDriver()
    {
        if ($this->driver !== null) {
            return;
        }

        foreach ($this->driversOrder as $preferredDriver) {
            if ($preferredDriver === self::DRIVER_REDIS && extension_loaded('redis')) {
                $this->driver = new Redis();
                return;
            }
            if ($preferredDriver === self::DRIVER_PREDIS && class_exists('Predis\Client')) {
                $this->driver = new Client();
                return;
            }
        }
        throw new RedisProxyException('No driver available');
    }

    protected function connect($host, $port, $timeout = null)
    {
        return $this->driver->connect($host, $port, $timeout);
    }

    protected function isConnected()
    {
        return $this->driver->isConnected();
    }

    /**
     * Returns null instead of false for Redis driver
     * @param mixed $result
     * @return mixed
     */
    protected function convertFalseToNull($result)
    {
        return $this->actualDriver() === self::DRIVER_REDIS && $result === false ? null : $result;
    }

    /**
     * Transforms Predis result Payload to boolean
     * @param mixed $result
     * @return mixed
     */
    protected function transformResult($result)
    {
        if ($this->actualDriver() === self::DRIVER_PREDIS && $result instanceof Status) {
            $result = $result->getPayload() === 'OK';
        }
        return $result;
    }

    /**
     * Create array from input array - odd keys are used as keys, even keys are used as values
     * @param array $dictionary
     * @param string $command
     * @return array
     * @throws RedisProxyException if number of keys is not the same as number of values
     */
    protected function prepareKeyValue(array $dictionary, $command)
    {
        $keys = array_values(array_filter($dictionary, function ($key) {
            return $key % 2 == 0;
        }, ARRAY_FILTER_USE_KEY));
        $values = array_values(array_filter($dictionary, function ($key) {
            return $key % 2 == 1;
        }, ARRAY_FILTER_USE_KEY));

        if (count($keys) != count($values)) {
            throw new RedisProxyException("Wrong number of arguments for $command command");
        }
        return array_combine($keys, $values);
    }

    protected function prepareArguments($command, ...$params)
    {
        if (!isset($params[0])) {
            throw new RedisProxyException("Wrong number of arguments for $command command");
        }
        if (is_array($params[0])) {
            $params = $params[0];
        }
        return $params;
    }
}
