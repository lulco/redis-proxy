<?php

namespace RedisProxy;

trait AbstractCommonBehavior
{
    protected $driver;

    abstract protected function init();

    abstract public function actualDriver();

    abstract protected function prepareDriver();

    abstract protected function connect($host, $port, $timeout = null);

    abstract protected function isConnected();

    abstract protected function convertFalseToNull($result);

    abstract protected function transformResult($result);

    abstract protected function prepareKeyValue(array $dictionary, $command);

    abstract protected function prepareArguments($command, ...$params);
}
