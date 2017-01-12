## Change Log

### [Unreleased][unreleased]

#### Changed
- Removed parameter database from constructor - BC break (use command select )
- Return values of some commands
- Unified info command for both drivers (inspired by predis)
- Return null instead of false on get and hget

#### Added
- Timeout to redis connect
- Possibility to set drivers order
- RedisProxyException - common exception for all drivers
- Predis commands

### [0.1.0] - 2016-04-17

#### Added
- Wrapper for \Redis

[unreleased]: https://github.com/lulco/redis-proxy/compare/0.1.0...HEAD
[0.1.0]: https://github.com/lulco/redis-proxy/compare/0.0.0...0.1.0
