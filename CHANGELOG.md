## Change Log

### [Unreleased][unreleased]

#### Changed
- removed parameter database from constructor - BC break (use command select )
- return values of some commands

#### Added
- timeout to redis connect
- possibility to set drivers order
- RedisProxyException - common exception for all drivers

#### Fixed
- Predis commands

### [0.1.0] - 2016-04-17
- First tagged version
- possible drivers: \Redis, \Predis\Client

[unreleased]: https://github.com/lulco/redis-proxy/compare/0.1.0...HEAD
[0.1.0]: https://github.com/lulco/redis-proxy/compare/0.0.0...0.1.0
