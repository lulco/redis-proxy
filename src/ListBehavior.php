<?php

namespace RedisProxy;

/**
 * @method int llen(string $key) Get the length of a list
 * @method array lrange(string $key, int $start, int $stop) Get a range of elements from a list
 */
trait ListBehavior
{
    private $driver;

    abstract protected function init();

    abstract public function actualDriver();

    /**
     * Prepend one or multiple values to a list
     * @param string $key
     * @param array $elements
     * @return int the length of the list after the push operations
     */
    public function lpush($key, ...$elements)
    {
        $elements = $this->prepareArguments('lpush', ...$elements);
        $this->init();
        return $this->driver->lpush($key, ...$elements);
    }

    /**
     * Append one or multiple values to a list
     * @param string $key
     * @param array $elements
     * @return int the length of the list after the push operations
     */
    public function rpush($key, ...$elements)
    {
        $elements = $this->prepareArguments('rpush', ...$elements);
        $this->init();
        return $this->driver->rpush($key, ...$elements);
    }

    /**
     * Remove and get the first element in a list
     * @param string $key
     * @return string|null
     */
    public function lpop($key)
    {
        $this->init();
        $result = $this->driver->lpop($key);
        return $this->convertFalseToNull($result);
    }

    /**
     * Remove and get the last element in a list
     * @param string $key
     * @return string|null
     */
    public function rpop($key)
    {
        $this->init();
        $result = $this->driver->rpop($key);
        return $this->convertFalseToNull($result);
    }

    /**
     * Get an element from a list by its index
     * @param string $key
     * @param int $index zero-based, so 0 means the first element, 1 the second element and so on. -1 means the last element, -2 means the penultimate and so forth
     * @return string|null
     */
    public function lindex($key, $index)
    {
        $this->init();
        $result = $this->driver->lindex($key, $index);
        return $this->convertFalseToNull($result);
    }
}
