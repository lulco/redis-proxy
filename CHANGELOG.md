## Change Log

### [Unreleased][unreleased]

### [0.2.1] - 2017-01-19
#### Fixed
- Fixed commands select, mget and hmget
 
### [0.2.0] - 2017-01-16
#### Changed
- Return values of some commands (e.g. return null instead of false on get and hget)
- Unified info command for both drivers (inspired by Predis)

#### Added
- Wrapper for \Predis\Client
- Timeout to redis connect
- Possibility to set drivers order
- RedisProxyException - common exception for all drivers

### [0.1.0] - 2016-04-17

#### Added
- Wrapper for \Redis

[unreleased]: https://github.com/lulco/redis-proxy/compare/0.2.1...HEAD
[0.2.1]: https://github.com/lulco/redis-proxy/compare/0.2.0...0.2.1
[0.2.0]: https://github.com/lulco/redis-proxy/compare/0.1.0...0.2.0
[0.1.0]: https://github.com/lulco/redis-proxy/compare/0.0.0...0.1.0
