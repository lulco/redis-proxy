<?php

declare(strict_types=1);

namespace RedisProxy\ConnectionFactory;

class Serializers
{
    public const NONE = 'none';
    public const PHP = 'php';
    public const JSON = 'json';
    public const MSGPACK = 'msgpack';
    public const IG_BINARY = 'igbinary';
}
