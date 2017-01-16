## Change Log

### [Unreleased][unreleased]

#### Changed
- Return values of some commands (e.g. return null instead of false on get and hget)
- Unified info command for both drivers (inspired by predis)

#### Added
- Wrapper for \Predis\Client
- Timeout to redis connect
- Possibility to set drivers order
- RedisProxyException - common exception for all drivers

### [0.1.0] - 2016-04-17

#### Added
- Wrapper for \Redis

[unreleased]: https://github.com/lulco/redis-proxy/compare/0.1.0...HEAD
[0.1.0]: https://github.com/lulco/redis-proxy/compare/0.0.0...0.1.0
