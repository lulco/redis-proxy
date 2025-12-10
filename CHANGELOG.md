# Change Log

## [Unreleased][unreleased]

## [1.7.0] - 2025-12-10
### Added
- Basic Stream support (xadd, xlen, xrange, xdel)

## [1.6.0] - 2025-05-20
### Added
- Multi write connections support
- Message serializer change support

## [1.5.0] - 2025-04-01
### Added
- connection reset method

## [1.4.0] - 2025-03-08
### Added
- Added rawCommand

## [1.3.1] - 2025-03-04
### Fixed
- Fixed getConnection() for MultiConnectionPool

## [1.3.0] - 2025-02-26
###  Added
- a few new commands: zincrby, hexpire, hexists, hstrlen
- Multi connection support

###  Fixed
- several commands fixed to work with redis 6.*, 7.* and dragonfly

## [1.2.0] - 2024-04-12
###  Added
- PHP 8.3 support

## [1.1.0] - 2024-02-02
###  Added
- Added publish, subscribe commands
- Added rename command
- Added srem, sismember commands

## [1.0.0] - 2023-09-06
###  Added
- Redis retries

## [0.7.1] - 2022-12-12
### Added
- Support for PHP 8.2

## [0.7.0] - 2022-06-30
### Added
- Support for predis/predis ^2.0
- Support for zpopmin, zpopmax with predis driver 2.0

## [0.6.0] - 2022-05-24
### Added
- Sentinels
- Sentinel add write to replicas config
- RedisProxyFactory

## [0.5.0] - 2022-05-05
### Changed
- Added typehints
- Dropped support for php < 7.4

### Added
- Added commands zrangebyscore, zpopmin, zpopmax

## [0.4.2] - 2020-06-25
### Fixed
- Fixed zrem for multiple members

## [0.4.1] - 2019-05-02
### Fixed
- Fixed typehint for hdel $key

## [0.4.0] - 2018-08-21
### Changed
- Dropped support for PHP 5.6 and 7.0 [BC break]

### Added
- Added commands: lset, lrem, zscan, zrem, zrank, zrevrank

## [0.3.1] - 2017-06-01
### Fixed
- Replaced Redis constants to integers

## [0.3.0] - 2017-06-01
### Added
- Added commands:
-- exists, type, dump, restore, setex, psetex, expire, pexpire, expireat, pexpireat, ttl, pttl, persist
-- getset, setnx, incr, incrby, incrbyfloat, decr, decrby, decrbyfloat
-- llen, lrange, lpush, rpush, lpop, rpop, lindex
-- zadd, zcard, zrange, zrevrange

### Fixed
- Fixed connect to not-default host / port with Predis\Client

## [0.2.1] - 2017-01-19
### Fixed
- Fixed commands select, mget and hmget
 
## [0.2.0] - 2017-01-16
### Changed
- Return values of some commands (e.g. return null instead of false on get and hget)
- Unified info command for both drivers (inspired by Predis)

### Added
- Wrapper for \Predis\Client
- Timeout to redis connect
- Possibility to set drivers order
- RedisProxyException - common exception for all drivers

## [0.1.0] - 2016-04-17

### Added
- Wrapper for \Redis

[unreleased]: https://github.com/lulco/redis-proxy/compare/1.7.0...HEAD
[1.7.0]: https://github.com/lulco/redis-proxy/compare/1.6.0...1.7.0
[1.6.0]: https://github.com/lulco/redis-proxy/compare/1.5.0...1.6.0
[1.5.0]: https://github.com/lulco/redis-proxy/compare/1.4.0...1.5.0
[1.4.0]: https://github.com/lulco/redis-proxy/compare/1.3.1...1.4.0
[1.3.1]: https://github.com/lulco/redis-proxy/compare/1.3.0...1.3.1
[1.3.0]: https://github.com/lulco/redis-proxy/compare/1.2.0...1.3.0
[1.2.0]: https://github.com/lulco/redis-proxy/compare/1.1.0...1.2.0
[1.1.0]: https://github.com/lulco/redis-proxy/compare/1.0.0...1.1.0
[1.0.0]: https://github.com/lulco/redis-proxy/compare/0.7.1...1.0.0
[0.7.1]: https://github.com/lulco/redis-proxy/compare/0.7.0...0.7.1
[0.7.0]: https://github.com/lulco/redis-proxy/compare/0.6.0...0.7.0
[0.6.0]: https://github.com/lulco/redis-proxy/compare/0.5.0...0.6.0
[0.5.0]: https://github.com/lulco/redis-proxy/compare/0.4.2...0.5.0
[0.4.2]: https://github.com/lulco/redis-proxy/compare/0.4.1...0.4.2
[0.4.1]: https://github.com/lulco/redis-proxy/compare/0.4.0...0.4.1
[0.4.0]: https://github.com/lulco/redis-proxy/compare/0.3.1...0.4.0
[0.3.1]: https://github.com/lulco/redis-proxy/compare/0.3.0...0.3.1
[0.3.0]: https://github.com/lulco/redis-proxy/compare/0.2.1...0.3.0
[0.2.1]: https://github.com/lulco/redis-proxy/compare/0.2.0...0.2.1
[0.2.0]: https://github.com/lulco/redis-proxy/compare/0.1.0...0.2.0
[0.1.0]: https://github.com/lulco/redis-proxy/compare/0.0.0...0.1.0
