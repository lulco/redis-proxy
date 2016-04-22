# Redis proxy
Library for creating redis instance depends on application / server possibilities

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
$redis = new \RedisProxy\RedisProxy($host, $port, $database);
$redis->hSet($hashKey, $key, $value);
$redis->hLen($hashKey);
$redis->hGet($hashKey, $key);
$redis->hGetAll($hashKey);
...

```
