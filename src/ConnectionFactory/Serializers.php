<?php

namespace RedisProxy\ConnectionFactory;

enum Serializers: string
{
    case NONE = 'none';
    case PHP = 'php';
    case JSON = 'json';
    case MSGPACK = 'msgpack';
    case IG_BINARY = 'igbinary';
}
