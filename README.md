# Redis proxy
Library for creating redis instance depends on application / server possibilities

[![Build Status](https://travis-ci.org/lulco/redis-proxy.svg?branch=master)](https://travis-ci.org/lulco/redis-proxy)
[![Scrutinizer Code Quality](https://scrutinizer-ci.com/g/lulco/redis-proxy/badges/quality-score.png?b=master)](https://scrutinizer-ci.com/g/lulco/redis-proxy/?branch=master)
[![Code Coverage](https://scrutinizer-ci.com/g/lulco/redis-proxy/badges/coverage.png?b=master)](https://scrutinizer-ci.com/g/lulco/redis-proxy/?branch=master)
[![SensioLabsInsight](https://insight.sensiolabs.com/projects/614109c1-261a-432a-9a32-50d7138b00c4/mini.png)](https://insight.sensiolabs.com/projects/614109c1-261a-432a-9a32-50d7138b00c4)
[![Latest Stable Version](https://img.shields.io/packagist/v/lulco/redis-proxy.svg)](https://packagist.org/packages/lulco/redis-proxy)
[![Total Downloads](https://img.shields.io/packagist/dt/lulco/redis-proxy.svg?style=flat-square)](https://packagist.org/packages/lulco/redis-proxy)
[![PHP 7 ready](http://php7ready.timesplinter.ch/lulco/redis-proxy/master/badge.svg)](https://travis-ci.org/lulco/redis-proxy)

## Installation

### Composer
The fastest way to install Redis proxy is to add it to your project using Composer (http://getcomposer.org/).

1. Install Composer:
    ```
    curl -sS https://getcomposer.org/installer | php
    ```
1. Require Redis proxy as a dependency using Composer:
    ```
    php composer.phar require lulco/redis-proxy
    ```
1. Install Redis proxy:
    ```
    php composer.phar update
    ```

## Usage
```
$redis = new \RedisProxy\RedisProxy($host, $port);
$redis->select($database);
$redis->hset($key, $field, $value);
$redis->hlen($key);
$redis->hget($key, $field);
$redis->hgetall($key);
...
```
