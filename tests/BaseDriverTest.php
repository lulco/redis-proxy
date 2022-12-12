<?php

namespace RedisProxy\Tests;

use PHPUnit\Framework\TestCase;
use RedisProxy\RedisProxy;
use RedisProxy\RedisProxyException;

abstract class BaseDriverTest extends TestCase
{
    protected RedisProxy $redisProxy;

    protected function setUp(): void
    {
        $this->redisProxy = $this->initializeDriver();
        $this->redisProxy->flushall();
    }

    abstract protected function initializeDriver(): RedisProxy;

    public function testSelect(): void
    {
        self::assertTrue($this->redisProxy->select(getenv('REDIS_PROXY_REDIS_DATABASE')));
        self::assertTrue($this->redisProxy->select(getenv('REDIS_PROXY_REDIS_DATABASE_2')));
    }

    public function testSelectInvalidDatabase()
    {
        $this->expectExceptionMessage("Invalid DB index");
        $this->expectException(RedisProxyException::class);
        $this->redisProxy->select(-1);
    }

    public function testInfo(): void
    {
        // no data
        $info = $this->redisProxy->info();
        self::assertTrue(is_array($info));
        self::assertArrayHasKey('server', $info);
        self::assertArrayHasKey('clients', $info);
        self::assertArrayHasKey('memory', $info);
        self::assertArrayHasKey('persistence', $info);
        self::assertArrayHasKey('stats', $info);
        self::assertArrayHasKey('replication', $info);
        self::assertArrayHasKey('cpu', $info);
        self::assertArrayHasKey('keyspace', $info);
        self::assertNotEmpty($info['keyspace']);

        $numberOfDatabases = $this->redisProxy->config('get', 'databases')['databases'];
        $keyspaceInfo = $this->redisProxy->info('keyspace');
        self::assertNotEmpty($keyspaceInfo);
        self::assertCount((int)$numberOfDatabases, $keyspaceInfo);
        foreach ($keyspaceInfo as $db => $dbValues) {
            self::assertStringStartsWith('db', $db);
            self::assertArrayHasKey('keys', $dbValues);
            self::assertEquals(0, $dbValues['keys']);
            self::assertArrayHasKey('expires', $dbValues);
            self::assertNull($dbValues['expires']);
            self::assertArrayHasKey('avg_ttl', $dbValues);
            self::assertNull($dbValues['avg_ttl']);
        }

        // insert some data
        $this->redisProxy->select(getenv('REDIS_PROXY_REDIS_DATABASE'));
        $this->redisProxy->set('first_key', 'first_value');

        $this->redisProxy->select(getenv('REDIS_PROXY_REDIS_DATABASE_2'));
        $this->redisProxy->set('second_key', 'second_value');
        $this->redisProxy->set('third_key', 'third_value');

        $numberOfDatabases = $this->redisProxy->config('get', 'databases')['databases'];
        $keyspaceInfo = $this->redisProxy->info('keyspace');
        self::assertNotEmpty($keyspaceInfo);
        self::assertCount((int)$numberOfDatabases, $keyspaceInfo);
        foreach ($keyspaceInfo as $db => $dbValues) {
            self::assertStringStartsWith('db', $db);
            self::assertArrayHasKey('keys', $dbValues);
            self::assertArrayHasKey('expires', $dbValues);
            self::assertArrayHasKey('avg_ttl', $dbValues);
        }
        self::assertArrayHasKey('db0', $keyspaceInfo);
        self::assertEquals(1, $keyspaceInfo['db0']['keys']);
        self::assertEquals(0, $keyspaceInfo['db0']['expires']);
        self::assertEquals(0, $keyspaceInfo['db0']['avg_ttl']);
        self::assertArrayHasKey('db1', $keyspaceInfo);
        self::assertEquals(2, $keyspaceInfo['db1']['keys']);
        self::assertEquals(0, $keyspaceInfo['db1']['expires']);
        self::assertEquals(0, $keyspaceInfo['db1']['avg_ttl']);

        $serverInfo = $this->redisProxy->info('server');
        self::assertNotEmpty($serverInfo);
    }

    public function testUnknownInfoSection(): void
    {
        $this->expectExceptionMessage("Info section \"unknown\" doesn't exist");
        $this->expectException(RedisProxyException::class);
        $this->redisProxy->info('unknown');
    }

    public function testDbSize(): void
    {
        self::assertEquals(0, $this->redisProxy->dbsize());
        self::assertTrue($this->redisProxy->set('my_key', 'my_value'));
        self::assertEquals(1, $this->redisProxy->dbsize());
        for ($i = 0; $i < 9; ++$i) {
            self::assertTrue($this->redisProxy->set("my_key_$i", "my_value$i"));
        }
        self::assertEquals(10, $this->redisProxy->dbsize());
        $keys = $this->redisProxy->keys('*');
        self::assertEquals(count($keys), $this->redisProxy->dbsize());
    }

    public function testKeys(): void
    {
        $keys = $this->redisProxy->keys('*');
        self::assertTrue(is_array($keys));
        self::assertEmpty($keys);
        self::assertTrue($this->redisProxy->set('my_key', 'my_value'));
        $keys = $this->redisProxy->keys('*');
        self::assertTrue(is_array($keys));
        self::assertEquals(['my_key'], $keys);
    }

    public function testKeysWithoutPattern(): void
    {
        $this->expectExceptionCode(1484162284);
        $this->expectExceptionMessage("Error for command 'keys', use getPrevious() for more info");
        $this->expectException(RedisProxyException::class);
        $this->redisProxy->keys();
    }

    public function testSetAndGet()
    {
        self::assertNull($this->redisProxy->get('my_key'));
        self::assertTrue($this->redisProxy->set('my_key', 'my_value'));
        self::assertEquals('my_value', $this->redisProxy->get('my_key'));
        self::assertTrue($this->redisProxy->set('my_key', 'my_new_value'));
        self::assertEquals('my_new_value', $this->redisProxy->get('my_key'));
    }

    public function testExists(): void
    {
        self::assertNull($this->redisProxy->get('my_key'));
        self::assertFalse($this->redisProxy->exists('my_key'));
        self::assertTrue($this->redisProxy->set('my_key', 'my_value'));
        self::assertEquals('my_value', $this->redisProxy->get('my_key'));
        self::assertTrue($this->redisProxy->exists('my_key'));
    }

    public function testDumpAndRestore(): void
    {
        self::assertNull($this->redisProxy->get('my_key_1'));
        self::assertNull($this->redisProxy->get('my_key_2'));
        self::assertNull($this->redisProxy->dump('my_key_1'));
        self::assertNull($this->redisProxy->dump('my_key_2'));
        self::assertTrue($this->redisProxy->set('my_key_1', 'my_value_1'));
        self::assertTrue($this->redisProxy->set('my_key_2', 'my_value_2'));
        self::assertEquals('my_value_1', $this->redisProxy->get('my_key_1'));
        self::assertEquals('my_value_2', $this->redisProxy->get('my_key_2'));
        $value1 = $this->redisProxy->dump('my_key_1');
        $value2 = $this->redisProxy->dump('my_key_2');
        self::assertEquals(2, $this->redisProxy->del('my_key_1', 'my_key_2'));
        self::assertNull($this->redisProxy->get('my_key_1'));
        self::assertNull($this->redisProxy->get('my_key_2'));
        self::assertTrue($this->redisProxy->restore('my_key_1', 0, $value2));
        self::assertTrue($this->redisProxy->restore('my_key_2', 0, $value1));
        self::assertEquals('my_value_2', $this->redisProxy->get('my_key_1'));
        self::assertEquals('my_value_1', $this->redisProxy->get('my_key_2'));
    }

    public function testExpire(): void
    {
        self::assertNull($this->redisProxy->get('my_key'));
        self::assertFalse($this->redisProxy->expire('my_key', 10));
        self::assertTrue($this->redisProxy->set('my_key', 'my_value'));
        self::assertTrue($this->redisProxy->expire('my_key', 10));
    }

    public function testPexpire(): void
    {
        self::assertNull($this->redisProxy->get('my_key'));
        self::assertFalse($this->redisProxy->pexpire('my_key', 10000));
        self::assertTrue($this->redisProxy->set('my_key', 'my_value'));
        self::assertTrue($this->redisProxy->pexpire('my_key', 10000));
    }

    public function testTtl(): void
    {
        self::assertNull($this->redisProxy->get('my_key'));
        self::assertEquals(-2, $this->redisProxy->ttl('my_key'));
        self::assertTrue($this->redisProxy->set('my_key', 'my_value'));
        self::assertEquals(-1, $this->redisProxy->ttl('my_key'));
        self::assertTrue($this->redisProxy->expire('my_key', 10));
        self::assertGreaterThanOrEqual(0, $this->redisProxy->ttl('my_key'));
        self::assertLessThanOrEqual(10, $this->redisProxy->ttl('my_key'));

        self::assertNull($this->redisProxy->get('my_key_2'));
        self::assertEquals(-2, $this->redisProxy->ttl('my_key_2'));
        self::assertTrue($this->redisProxy->set('my_key_2', 'my_value'));
        self::assertEquals(-1, $this->redisProxy->ttl('my_key_2'));
        self::assertTrue($this->redisProxy->pexpire('my_key_2', 10000));
        self::assertGreaterThanOrEqual(0, $this->redisProxy->ttl('my_key_2'));
        self::assertLessThanOrEqual(10, $this->redisProxy->ttl('my_key_2'));
    }

    public function testExpireat(): void
    {
        self::assertNull($this->redisProxy->get('my_key'));
        self::assertFalse($this->redisProxy->expireat('my_key', time() + 10));
        self::assertTrue($this->redisProxy->set('my_key', 'my_value'));
        self::assertTrue($this->redisProxy->expireat('my_key', time() + 10));
        self::assertGreaterThanOrEqual(0, $this->redisProxy->ttl('my_key'));
        self::assertLessThanOrEqual(10, $this->redisProxy->ttl('my_key'));
    }

    public function testPexpireat(): void
    {
        self::assertNull($this->redisProxy->get('my_key'));
        self::assertFalse($this->redisProxy->pexpireat('my_key', (time() + 10) * 1000));
        self::assertTrue($this->redisProxy->set('my_key', 'my_value'));
        self::assertTrue($this->redisProxy->pexpireat('my_key', (time() + 10) * 1000));
        self::assertGreaterThanOrEqual(0, $this->redisProxy->ttl('my_key'));
        self::assertLessThanOrEqual(10, $this->redisProxy->ttl('my_key'));
        self::assertGreaterThanOrEqual(0, $this->redisProxy->pttl('my_key'));
        self::assertLessThanOrEqual(10000, $this->redisProxy->pttl('my_key'));
    }

    public function testPersist(): void
    {
        self::assertNull($this->redisProxy->get('my_key'));
        self::assertFalse($this->redisProxy->persist('my_key'));
        self::assertTrue($this->redisProxy->set('my_key', 'my_value'));
        self::assertFalse($this->redisProxy->persist('my_key'));
        self::assertEquals(-1, $this->redisProxy->ttl('my_key'));
        self::assertTrue($this->redisProxy->expire('my_key', 10));
        self::assertGreaterThanOrEqual(0, $this->redisProxy->ttl('my_key'));
        self::assertLessThanOrEqual(10, $this->redisProxy->ttl('my_key'));
        self::assertTrue($this->redisProxy->persist('my_key'));
        self::assertEquals(-1, $this->redisProxy->ttl('my_key'));
    }

    public function testGetSet(): void
    {
        self::assertNull($this->redisProxy->get('my_key'));
        self::assertNull($this->redisProxy->getset('my_key', 'my_value'));
        self::assertEquals('my_value', $this->redisProxy->getset('my_key', 'my_new_value'));
        self::assertEquals('my_new_value', $this->redisProxy->get('my_key'));
    }

    public function testSetnx(): void
    {
        self::assertNull($this->redisProxy->get('my_key'));
        self::assertTrue($this->redisProxy->setnx('my_key', 'my_value'));
        self::assertEquals('my_value', $this->redisProxy->get('my_key'));
        self::assertFalse($this->redisProxy->setnx('my_key', 'my_new_value'));
        self::assertEquals('my_value', $this->redisProxy->get('my_key'));
    }

    public function testSetex(): void
    {
        self::assertNull($this->redisProxy->get('my_key'));
        self::assertTrue($this->redisProxy->set('my_key', 'my_value'));
        self::assertEquals(-1, $this->redisProxy->ttl('my_key'));
        self::assertTrue($this->redisProxy->setex('my_key', 10, 'my_value'));
        self::assertGreaterThanOrEqual(0, $this->redisProxy->ttl('my_key'));
        self::assertLessThanOrEqual(10, $this->redisProxy->ttl('my_key'));
    }

    public function testPsetex(): void
    {
        self::assertNull($this->redisProxy->get('my_key'));
        self::assertTrue($this->redisProxy->set('my_key', 'my_value'));
        self::assertEquals(-1, $this->redisProxy->ttl('my_key'));
        self::assertTrue($this->redisProxy->psetex('my_key', 10000, 'my_value'));
        self::assertGreaterThanOrEqual(0, $this->redisProxy->ttl('my_key'));
        self::assertLessThanOrEqual(10, $this->redisProxy->ttl('my_key'));
        self::assertGreaterThanOrEqual(0, $this->redisProxy->pttl('my_key'));
        self::assertLessThanOrEqual(10000, $this->redisProxy->pttl('my_key'));
    }

    public function testMset(): void
    {
        self::assertEquals(0, $this->redisProxy->dbsize());
        self::assertTrue($this->redisProxy->mset('my_key_1', 'my_value_1', 'my_key_2', 'my_value_2'));
        self::assertEquals(2, $this->redisProxy->dbsize());
        self::assertEquals('my_value_1', $this->redisProxy->get('my_key_1'));
        self::assertEquals('my_value_2', $this->redisProxy->get('my_key_2'));
        self::assertEquals(2, $this->redisProxy->del('my_key_1', 'my_key_2'));
        self::assertEquals(0, $this->redisProxy->dbsize());

        self::assertTrue($this->redisProxy->mset(['my_key_1' => 'my_value_1', 'my_key_2' => 'my_value_2']));
        self::assertEquals(2, $this->redisProxy->dbsize());
        self::assertEquals('my_value_1', $this->redisProxy->get('my_key_1'));
        self::assertEquals('my_value_2', $this->redisProxy->get('my_key_2'));
        self::assertEquals(2, $this->redisProxy->del('my_key_1', 'my_key_2'));
        self::assertEquals(0, $this->redisProxy->dbsize());
    }

    public function testWrongNumberOfArgumentsMset(): void
    {
        $this->expectExceptionMessage("Wrong number of arguments for mset command");
        $this->expectException(RedisProxyException::class);
        $this->redisProxy->mset('my_key_1', 'my_value_1', 'my_key_2', 'my_value_2', 'xxx');
    }

    public function testMget(): void
    {
        // all non existing keys
        $mget = $this->redisProxy->mget(['first_key', 'second_key']);
        self::assertCount(2, $mget);
        self::assertArrayHasKey('first_key', $mget);
        self::assertNull($mget['first_key']);
        self::assertArrayHasKey('second_key', $mget);
        self::assertNull($mget['second_key']);

        // all existing keys
        self::assertTrue($this->redisProxy->set('first_key', 'first_value'));
        self::assertTrue($this->redisProxy->set('second_key', 'second_value'));
        $mget = $this->redisProxy->mget(['first_key', 'second_key']);
        self::assertCount(2, $mget);
        self::assertArrayHasKey('first_key', $mget);
        self::assertEquals('first_value', $mget['first_key']);
        self::assertArrayHasKey('second_key', $mget);
        self::assertEquals('second_value', $mget['second_key']);

        // some existing and some non existing keys
        $mget = $this->redisProxy->mget(['first_key', 'second_key', 'third_key']);
        self::assertCount(3, $mget);
        self::assertArrayHasKey('first_key', $mget);
        self::assertEquals('first_value', $mget['first_key']);
        self::assertArrayHasKey('second_key', $mget);
        self::assertEquals('second_value', $mget['second_key']);
        self::assertArrayHasKey('third_key', $mget);
        self::assertNull($mget['third_key']);

        // all existing keys
        self::assertTrue($this->redisProxy->set('third_key', 'third_value'));
        $mget = $this->redisProxy->mget(['first_key', 'second_key', 'third_key']);
        self::assertCount(3, $mget);
        self::assertArrayHasKey('first_key', $mget);
        self::assertEquals('first_value', $mget['first_key']);
        self::assertArrayHasKey('second_key', $mget);
        self::assertEquals('second_value', $mget['second_key']);
        self::assertArrayHasKey('third_key', $mget);
        self::assertEquals('third_value', $mget['third_key']);

        // duplicate key in mget
        $mget = $this->redisProxy->mget(['first_key', 'first_key', 'second_key', 'third_key']);
        self::assertCount(3, $mget);
        self::assertArrayHasKey('first_key', $mget);
        self::assertEquals('first_value', $mget['first_key']);
        self::assertArrayHasKey('second_key', $mget);
        self::assertEquals('second_value', $mget['second_key']);
        self::assertArrayHasKey('third_key', $mget);
        self::assertEquals('third_value', $mget['third_key']);

        // argument not array
        $mget = $this->redisProxy->mget('first_key', 'second_key', 'third_key');
        self::assertCount(3, $mget);
        self::assertArrayHasKey('first_key', $mget);
        self::assertEquals('first_value', $mget['first_key']);
        self::assertArrayHasKey('second_key', $mget);
        self::assertEquals('second_value', $mget['second_key']);
        self::assertArrayHasKey('third_key', $mget);
        self::assertEquals('third_value', $mget['third_key']);
    }

    public function testDelete(): void
    {
        self::assertEquals(0, $this->redisProxy->delete('my_key'));
        self::assertTrue($this->redisProxy->set('my_key', 'my_value'));
        self::assertEquals(1, $this->redisProxy->delete('my_key'));
        self::assertNull($this->redisProxy->get('my_key'));

        self::assertEquals(0, $this->redisProxy->del('my_key'));
        self::assertTrue($this->redisProxy->set('my_key', 'my_value'));
        self::assertEquals(1, $this->redisProxy->del('my_key'));
        self::assertNull($this->redisProxy->get('my_key'));

        self::assertEquals(0, $this->redisProxy->del(['my_key']));
        self::assertTrue($this->redisProxy->set('my_key', 'my_value'));
        self::assertTrue($this->redisProxy->set('my_key_2', 'my_value_2'));
        self::assertEquals(2, $this->redisProxy->del(['my_key', 'my_key_2']));
        self::assertNull($this->redisProxy->get('my_key'));
        self::assertNull($this->redisProxy->get('my_key_2'));
    }

    public function testMultipleDelete(): void
    {
        self::assertEquals(0, $this->redisProxy->delete('first_key', 'second_key', 'third_key'));
        self::assertNull($this->redisProxy->get('first_key'));
        self::assertNull($this->redisProxy->get('second_key'));
        self::assertNull($this->redisProxy->get('third_key'));

        self::assertTrue($this->redisProxy->set('first_key', 'first_value'));
        self::assertTrue($this->redisProxy->set('second_key', 'second_value'));
        self::assertTrue($this->redisProxy->set('third_key', 'third_value'));
        self::assertEquals(3, $this->redisProxy->delete('first_key', 'second_key', 'third_key'));
        self::assertNull($this->redisProxy->get('first_key'));
        self::assertNull($this->redisProxy->get('second_key'));
        self::assertNull($this->redisProxy->get('third_key'));

        self::assertTrue($this->redisProxy->set('second_key', 'second_value'));
        self::assertEquals(1, $this->redisProxy->delete(['first_key', 'second_key', 'third_key']));
        self::assertNull($this->redisProxy->get('first_key'));
        self::assertNull($this->redisProxy->get('second_key'));
        self::assertNull($this->redisProxy->get('third_key'));
    }

    public function testDelNoKeys(): void
    {
        $this->expectExceptionMessage("Wrong number of arguments for del command");
        $this->expectException(RedisProxyException::class);
        $this->redisProxy->del();
    }

    public function testDeleteNoKeys(): void
    {
        $this->expectExceptionMessage("Wrong number of arguments for del command");
        $this->expectException(RedisProxyException::class);
        $this->redisProxy->delete();
    }

    public function testIncr(): void
    {
        self::assertNull($this->redisProxy->get('my_key'));
        self::assertEquals(1, $this->redisProxy->incr('my_key'));
        self::assertEquals(1, $this->redisProxy->get('my_key'));
        self::assertEquals(2, $this->redisProxy->incr('my_key', 5));
        self::assertEquals(2, $this->redisProxy->get('my_key'));
        self::assertEquals(3, $this->redisProxy->incr('my_key', -4.1234));
        self::assertEquals(3, $this->redisProxy->get('my_key'));
    }

    public function testIncrby(): void
    {
        self::assertNull($this->redisProxy->get('my_key'));
        self::assertEquals(1, $this->redisProxy->incrby('my_key'));
        self::assertEquals(1, $this->redisProxy->get('my_key'));
        self::assertEquals(6, $this->redisProxy->incrby('my_key', 5));
        self::assertEquals(6, $this->redisProxy->get('my_key'));
        self::assertEquals(10, $this->redisProxy->incrby('my_key', 4.1234));
        self::assertEquals(10, $this->redisProxy->get('my_key'));
        self::assertEquals(5, $this->redisProxy->incrby('my_key', -5));
        self::assertEquals(5, $this->redisProxy->get('my_key'));
        self::assertEquals(0, $this->redisProxy->incrby('my_key', -5));
        self::assertEquals(0, $this->redisProxy->get('my_key'));
    }

    public function testIncrbyfloat(): void
    {
        self::assertNull($this->redisProxy->get('my_key'));
        self::assertEquals(1, $this->redisProxy->incrbyfloat('my_key'));
        self::assertEquals(1, $this->redisProxy->get('my_key'));
        self::assertEquals(6, $this->redisProxy->incrbyfloat('my_key', 5));
        self::assertEquals(6, $this->redisProxy->get('my_key'));
        self::assertEquals(10.1234, round($this->redisProxy->incrbyfloat('my_key', 4.1234), 4));
        self::assertEquals(10.1234, round($this->redisProxy->get('my_key'), 4));
        self::assertEquals(5.1234, round($this->redisProxy->incrbyfloat('my_key', -5), 4));
        self::assertEquals(5.1234, round($this->redisProxy->get('my_key'), 4));
        self::assertEquals(-1.4, round($this->redisProxy->incrbyfloat('my_key', -6.5234), 1));
        self::assertEquals(-1.4, round($this->redisProxy->get('my_key'), 1));
    }

    public function testDecr(): void
    {
        self::assertNull($this->redisProxy->get('my_key'));
        self::assertTrue($this->redisProxy->set('my_key', 10));
        self::assertEquals(10, $this->redisProxy->get('my_key'));
        self::assertEquals(9, $this->redisProxy->decr('my_key'));
        self::assertEquals(9, $this->redisProxy->get('my_key'));
        self::assertEquals(8, $this->redisProxy->decr('my_key', 5));
        self::assertEquals(8, $this->redisProxy->get('my_key'));
        self::assertEquals(7, $this->redisProxy->decr('my_key', -4.1234));
        self::assertEquals(7, $this->redisProxy->get('my_key'));
    }

    public function testDecrby(): void
    {
        self::assertNull($this->redisProxy->get('my_key'));
        self::assertTrue($this->redisProxy->set('my_key', 10));
        self::assertEquals(10, $this->redisProxy->get('my_key'));
        self::assertEquals(9, $this->redisProxy->decrby('my_key'));
        self::assertEquals(9, $this->redisProxy->get('my_key'));
        self::assertEquals(4, $this->redisProxy->decrby('my_key', 5));
        self::assertEquals(4, $this->redisProxy->get('my_key'));
        self::assertEquals(0, $this->redisProxy->decrby('my_key', 4.1234));
        self::assertEquals(0, $this->redisProxy->get('my_key'));
        self::assertEquals(5, $this->redisProxy->decrby('my_key', -5));
        self::assertEquals(5, $this->redisProxy->get('my_key'));
        self::assertEquals(10, $this->redisProxy->decrby('my_key', -5));
        self::assertEquals(10, $this->redisProxy->get('my_key'));
    }

    public function testDecrbyfloat(): void
    {
        self::assertNull($this->redisProxy->get('my_key'));
        self::assertTrue($this->redisProxy->set('my_key', 10));
        self::assertEquals(10, $this->redisProxy->get('my_key'));
        self::assertEquals(9, $this->redisProxy->decrbyfloat('my_key'));
        self::assertEquals(9, $this->redisProxy->get('my_key'));
        self::assertEquals(4, $this->redisProxy->decrbyfloat('my_key', 5));
        self::assertEquals(4, $this->redisProxy->get('my_key'));
        self::assertEquals(-0.1234, round($this->redisProxy->decrbyfloat('my_key', 4.1234), 4));
        self::assertEquals(-0.1234, round($this->redisProxy->get('my_key'), 4));
        self::assertEquals(4.8766, round($this->redisProxy->decrbyfloat('my_key', -5), 4));
        self::assertEquals(4.8766, round($this->redisProxy->get('my_key'), 4));
        self::assertEquals(11.4, round($this->redisProxy->decrbyfloat('my_key', -6.5234), 1));
        self::assertEquals(11.4, round($this->redisProxy->get('my_key'), 1));
    }

    public function testScan(): void
    {
        self::assertEquals(0, $this->redisProxy->dbsize());
        $keys = [];
        for ($i = 0; $i < 1000; ++$i) {
            $keys["my_key_$i"] = "my_value_$i";
        }
        self::assertTrue($this->redisProxy->mset($keys));
        self::assertEquals(1000, $this->redisProxy->dbsize());

        $count = 0;
        $iterator = null;
        while ($scanKeys = $this->redisProxy->scan($iterator, null, 100)) {
            $count += count($scanKeys);
            foreach ($scanKeys as $key) {
                self::assertTrue(strpos($key, 'my_key_') === 0);
            }
        }
        self::assertEquals(1000, $count);
        self::assertEquals(0, $iterator);

        $count = 0;
        $iterator = null;
        while ($scanKeys = $this->redisProxy->scan($iterator, 'my_key_1*', 100)) {
            $count += count($scanKeys);
            foreach ($scanKeys as $key) {
                self::assertTrue(strpos($key, 'my_key_1') === 0);
            }
        }
        self::assertEquals(111, $count);
        self::assertEquals(0, $iterator);

        $count = 0;
        $iterator = null;
        while ($scanKeys = $this->redisProxy->scan($iterator, '*1*', 100)) {
            $count += count($scanKeys);
            foreach ($scanKeys as $key) {
                self::assertTrue(strpos($key, '1') !== false);
            }
        }
        self::assertEquals(271, $count);
        self::assertEquals(0, $iterator);
    }

    public function testHset(): void
    {
        self::assertNull($this->redisProxy->hget('my_hash_key', 'my_field'));
        self::assertEquals(1, $this->redisProxy->hset('my_hash_key', 'my_field', 'my_value'));
        self::assertEquals('my_value', $this->redisProxy->hget('my_hash_key', 'my_field'));
        self::assertEquals(0, $this->redisProxy->hset('my_hash_key', 'my_field', 'my_new_value'));
        self::assertEquals('my_new_value', $this->redisProxy->hget('my_hash_key', 'my_field'));
    }

    public function testHgetall(): void
    {
        self::assertTrue(is_array($this->redisProxy->hgetall('my_hash_key')));
        self::assertCount(0, $this->redisProxy->hgetall('my_hash_key'));
        self::assertEquals([], $this->redisProxy->hgetall('my_hash_key'));
        self::assertEquals(1, $this->redisProxy->hset('my_hash_key', 'my_first_field', 'my_first_value'));
        self::assertEquals(1, $this->redisProxy->hset('my_hash_key', 'my_second_field', 'my_second_value'));
        self::assertTrue(is_array($this->redisProxy->hgetall('my_hash_key')));
        self::assertCount(2, $this->redisProxy->hgetall('my_hash_key'));
        self::assertArrayHasKey('my_first_field', $this->redisProxy->hgetall('my_hash_key'));
        self::assertArrayHasKey('my_second_field', $this->redisProxy->hgetall('my_hash_key'));
        self::assertArrayNotHasKey('my_third_field', $this->redisProxy->hgetall('my_hash_key'));
    }

    public function testHlen(): void
    {
        self::assertEquals(0, $this->redisProxy->hlen('my_hash_key'));
        self::assertEquals(1, $this->redisProxy->hset('my_hash_key', 'my_first_field', 'my_first_value'));
        self::assertEquals(1, $this->redisProxy->hlen('my_hash_key'));
        self::assertEquals(1, $this->redisProxy->hset('my_hash_key', 'my_second_field', 'my_second_value'));
        self::assertEquals(2, $this->redisProxy->hlen('my_hash_key'));
        self::assertEquals(1, $this->redisProxy->hdel('my_hash_key', 'my_first_field'));
        self::assertEquals(1, $this->redisProxy->hlen('my_hash_key'));
    }

    public function testHkeys(): void
    {
        self::assertEquals(0, $this->redisProxy->hlen('my_hash_key'));
        self::assertTrue(is_array($this->redisProxy->hkeys('my_hash_key')));
        self::assertCount(0, $this->redisProxy->hkeys('my_hash_key'));
        self::assertEquals([], $this->redisProxy->hkeys('my_hash_key'));
        self::assertEquals(1, $this->redisProxy->hset('my_hash_key', 'my_first_field', 'my_first_value'));
        self::assertEquals(1, $this->redisProxy->hset('my_hash_key', 'my_second_field', 'my_second_value'));
        self::assertEquals(['my_first_field', 'my_second_field'], $this->redisProxy->hkeys('my_hash_key'));
    }

    public function testHdel(): void
    {
        self::assertEquals(1, $this->redisProxy->hset('my_hash_key', 'my_first_field', 'my_first_value'));
        self::assertEquals(1, $this->redisProxy->hset('my_hash_key', 'my_second_field', 'my_second_value'));
        self::assertEquals(2, $this->redisProxy->hlen('my_hash_key'));
        self::assertEquals(1, $this->redisProxy->del('my_hash_key'));
        self::assertEquals(0, $this->redisProxy->hlen('my_hash_key'));

        self::assertEquals(1, $this->redisProxy->hset('my_hash_key', 'my_first_field', 'my_first_value'));
        self::assertEquals(1, $this->redisProxy->hset('my_hash_key', 'my_second_field', 'my_second_value'));
        self::assertEquals(2, $this->redisProxy->hlen('my_hash_key'));
        // field as string
        self::assertEquals(1, $this->redisProxy->hdel('my_hash_key', 'my_first_field'));
        self::assertEquals(1, $this->redisProxy->hlen('my_hash_key'));
        // list of fields
        self::assertEquals(1, $this->redisProxy->hdel('my_hash_key', 'my_first_field', 'my_second_field'));
        self::assertEquals(0, $this->redisProxy->hlen('my_hash_key'));

        self::assertEquals(1, $this->redisProxy->hset('my_hash_key', 'my_first_field', 'my_first_value'));
        self::assertEquals(1, $this->redisProxy->hset('my_hash_key', 'my_second_field', 'my_second_value'));
        self::assertEquals(2, $this->redisProxy->hlen('my_hash_key'));
        self::assertEquals(2, $this->redisProxy->hdel('my_hash_key', 'my_first_field', 'my_second_field'));
        self::assertEquals(0, $this->redisProxy->hlen('my_hash_key'));

        // delete non existing fields
        self::assertEquals(0, $this->redisProxy->hdel('my_hash_key', 'my_first_field', 'my_second_field'));

        self::assertEquals(1, $this->redisProxy->hset('my_hash_key', 'my_first_field', 'my_first_value'));
        self::assertEquals(1, $this->redisProxy->hset('my_hash_key', 'my_second_field', 'my_second_value'));
        self::assertEquals(2, $this->redisProxy->hlen('my_hash_key'));
        // fields as array
        self::assertEquals(2, $this->redisProxy->hdel('my_hash_key', ['my_first_field', 'my_second_field']));
        self::assertEquals(0, $this->redisProxy->hlen('my_hash_key'));

        self::assertEquals(1, $this->redisProxy->hset('my_hash_key', 'my_first_field', 'my_first_value'));
        self::assertEquals(1, $this->redisProxy->hset('my_hash_key', 'my_second_field', 'my_second_value'));
        self::assertEquals(2, $this->redisProxy->hlen('my_hash_key'));
        // each fields as array - only first argument is accepted
        self::assertEquals(1, $this->redisProxy->hdel('my_hash_key', ['my_first_field'], ['my_second_field']));
        self::assertEquals(1, $this->redisProxy->hlen('my_hash_key'));
    }

    public function testHincrby(): void
    {
        self::assertEquals(0, $this->redisProxy->hget('my_hash_key', 'my_incr_field'));
        self::assertEquals(1, $this->redisProxy->hincrby('my_hash_key', 'my_incr_field'));
        self::assertEquals(1, $this->redisProxy->hget('my_hash_key', 'my_incr_field'));
        self::assertEquals(6, $this->redisProxy->hincrby('my_hash_key', 'my_incr_field', 5));
        self::assertEquals(6, $this->redisProxy->hget('my_hash_key', 'my_incr_field'));
        self::assertEquals(10, $this->redisProxy->hincrby('my_hash_key', 'my_incr_field', 4.1234));
        self::assertEquals(10, $this->redisProxy->hget('my_hash_key', 'my_incr_field'));
        self::assertEquals(5, $this->redisProxy->hincrby('my_hash_key', 'my_incr_field', -5));
        self::assertEquals(5, $this->redisProxy->hget('my_hash_key', 'my_incr_field'));
    }

    public function testHincrbyFloat(): void
    {
        self::assertEquals(0, $this->redisProxy->hget('my_hash_key', 'my_incr_field'));
        self::assertEquals(1, $this->redisProxy->hincrbyfloat('my_hash_key', 'my_incr_field'));
        self::assertEquals(1, $this->redisProxy->hget('my_hash_key', 'my_incr_field'));
        self::assertEquals(6, $this->redisProxy->hincrbyfloat('my_hash_key', 'my_incr_field', 5));
        self::assertEquals(6, $this->redisProxy->hget('my_hash_key', 'my_incr_field'));
        self::assertEquals(10.1234, $this->redisProxy->hincrbyfloat('my_hash_key', 'my_incr_field', 4.1234));
        self::assertEquals(10.1234, $this->redisProxy->hget('my_hash_key', 'my_incr_field'));
        self::assertEquals(9.1, $this->redisProxy->hincrbyfloat('my_hash_key', 'my_incr_field', -1.0234));
        self::assertEquals(9.1, $this->redisProxy->hget('my_hash_key', 'my_incr_field'));
    }

    public function testHmset(): void
    {
        self::assertEquals(0, $this->redisProxy->hlen('my_hash_key'));

        self::assertTrue($this->redisProxy->hmset('my_hash_key', 'my_field_1', 'my_value_1', 'my_field_2', 'my_value_2'));
        self::assertEquals(2, $this->redisProxy->hlen('my_hash_key'));
        self::assertEquals('my_value_1', $this->redisProxy->hget('my_hash_key', 'my_field_1'));
        self::assertEquals('my_value_2', $this->redisProxy->hget('my_hash_key', 'my_field_2'));
        self::assertEquals(1, $this->redisProxy->del('my_hash_key'));

        self::assertTrue($this->redisProxy->hmset('my_hash_key', ['my_field_1' => 'my_value_1', 'my_field_2' => 'my_value_2']));
        self::assertEquals(2, $this->redisProxy->hlen('my_hash_key'));
        self::assertEquals('my_value_1', $this->redisProxy->hget('my_hash_key', 'my_field_1'));
        self::assertEquals('my_value_2', $this->redisProxy->hget('my_hash_key', 'my_field_2'));
        self::assertEquals(1, $this->redisProxy->del('my_hash_key'));
    }

    public function testWrongNumberOfArgumentsHmset(): void
    {
        $this->expectExceptionMessage("Wrong number of arguments for hmset command");
        $this->expectException(RedisProxyException::class);
        $this->redisProxy->hmset('my_hash_key', 'my_field_1', 'my_value_1', 'my_field_2', 'my_value_2', 'xxx');
    }

    public function testHmget(): void
    {
        // all non existing keys
        $hmget = $this->redisProxy->hmget('my_hash_key', ['first_field', 'second_field']);
        self::assertCount(2, $hmget);
        self::assertArrayHasKey('first_field', $hmget);
        self::assertNull($hmget['first_field']);
        self::assertArrayHasKey('second_field', $hmget);
        self::assertNull($hmget['second_field']);

        // all existing keys
        self::assertEquals(1, $this->redisProxy->hset('my_hash_key', 'first_field', 'first_value'));
        self::assertEquals(1, $this->redisProxy->hset('my_hash_key', 'second_field', 'second_value'));
        $hmget = $this->redisProxy->hmget('my_hash_key', ['first_field', 'second_field']);
        self::assertCount(2, $hmget);
        self::assertArrayHasKey('first_field', $hmget);
        self::assertEquals('first_value', $hmget['first_field']);
        self::assertArrayHasKey('second_field', $hmget);
        self::assertEquals('second_value', $hmget['second_field']);

        // some existing and some non existing keys
        $hmget = $this->redisProxy->hmget('my_hash_key', ['first_field', 'second_field', 'third_field']);
        self::assertCount(3, $hmget);
        self::assertArrayHasKey('first_field', $hmget);
        self::assertEquals('first_value', $hmget['first_field']);
        self::assertArrayHasKey('second_field', $hmget);
        self::assertEquals('second_value', $hmget['second_field']);
        self::assertArrayHasKey('third_field', $hmget);
        self::assertNull($hmget['third_field']);

        // all existing keys
        self::assertEquals(1, $this->redisProxy->hset('my_hash_key', 'third_field', 'third_value'));
        $hmget = $this->redisProxy->hmget('my_hash_key', ['first_field', 'second_field', 'third_field']);
        self::assertCount(3, $hmget);
        self::assertArrayHasKey('first_field', $hmget);
        self::assertEquals('first_value', $hmget['first_field']);
        self::assertArrayHasKey('second_field', $hmget);
        self::assertEquals('second_value', $hmget['second_field']);
        self::assertArrayHasKey('third_field', $hmget);
        self::assertEquals('third_value', $hmget['third_field']);

        // duplicate key in mget
        $hmget = $this->redisProxy->hmget('my_hash_key', ['first_field', 'first_field', 'second_field', 'third_field']);
        self::assertCount(3, $hmget);
        self::assertArrayHasKey('first_field', $hmget);
        self::assertEquals('first_value', $hmget['first_field']);
        self::assertArrayHasKey('second_field', $hmget);
        self::assertEquals('second_value', $hmget['second_field']);
        self::assertArrayHasKey('third_field', $hmget);
        self::assertEquals('third_value', $hmget['third_field']);

        // argument not array
        $hmget = $this->redisProxy->hmget('my_hash_key', 'first_field', 'second_field', 'third_field');
        self::assertCount(3, $hmget);
        self::assertArrayHasKey('first_field', $hmget);
        self::assertEquals('first_value', $hmget['first_field']);
        self::assertArrayHasKey('second_field', $hmget);
        self::assertEquals('second_value', $hmget['second_field']);
        self::assertArrayHasKey('third_field', $hmget);
        self::assertEquals('third_value', $hmget['third_field']);
    }

    public function testHscan(): void
    {
        self::assertEquals(0, $this->redisProxy->hlen('my_hash_key'));
        $members = [];
        for ($i = 0; $i < 1000; ++$i) {
            $members["my_field_$i"] = "my_value_$i";
        }
        self::assertTrue($this->redisProxy->hmset('my_hash_key', $members));
        self::assertEquals(1000, $this->redisProxy->hlen('my_hash_key'));

        $count = 0;
        $iterator = null;
        while ($hscanMembers = $this->redisProxy->hscan('my_hash_key', $iterator, null, 100)) {
            $count += count($hscanMembers);
            foreach ($hscanMembers as $field => $value) {
                self::assertTrue(strpos($field, 'my_field_') === 0);
                self::assertTrue(strpos($value, 'my_value_') === 0);
            }
        }
        self::assertEquals(1000, $count);
        self::assertEquals(0, $iterator);

        $count = 0;
        $iterator = null;
        while ($hscanMembers = $this->redisProxy->hscan('my_hash_key', $iterator, 'my_field_1*', 100)) {
            $count += count($hscanMembers);
            foreach ($hscanMembers as $field => $value) {
                self::assertTrue(strpos($field, 'my_field_1') === 0);
                self::assertTrue(strpos($value, 'my_value_1') === 0);
            }
        }
        self::assertEquals(111, $count);
        self::assertEquals(0, $iterator);

        $count = 0;
        $iterator = null;
        while ($hscanMembers = $this->redisProxy->hscan('my_hash_key', $iterator, '*1*', 100)) {
            $count += count($hscanMembers);
            foreach ($hscanMembers as $field => $value) {
                self::assertTrue(strpos($field, '1') !== false);
                self::assertTrue(strpos($value, '1') !== false);
            }
        }
        self::assertEquals(271, $count);
        self::assertEquals(0, $iterator);
    }

    public function testSadd(): void
    {
        // add one member
        self::assertEquals(1, $this->redisProxy->sadd('my_set_key', 'member'));
        self::assertEquals(1, $this->redisProxy->scard('my_set_key'));

        // add three members
        self::assertEquals(3, $this->redisProxy->sadd('my_set_key', 'member_2', 'member_3', 'member_4'));
        self::assertEquals(4, $this->redisProxy->scard('my_set_key'));

        // add members which are already in set
        self::assertEquals(0, $this->redisProxy->sadd('my_set_key', 'member_2', 'member_3', 'member_4'));
        self::assertEquals(4, $this->redisProxy->scard('my_set_key'));

        // add some new members and some members which are already in set
        self::assertEquals(2, $this->redisProxy->sadd('my_set_key', 'member_2', 'member_3', 'member_5', 'member_6'));
        self::assertEquals(6, $this->redisProxy->scard('my_set_key'));

        // add three members as array
        self::assertEquals(3, $this->redisProxy->sadd('my_set_key', ['member_7', 'member_8', 'member_9']));
        self::assertEquals(9, $this->redisProxy->scard('my_set_key'));
    }

    public function testSpop(): void
    {
        self::assertEquals(0, $this->redisProxy->scard('my_set_key'));
        self::assertNull($this->redisProxy->spop('my_set_key'));
        self::assertEquals(4, $this->redisProxy->sadd('my_set_key', 'member_1', 'member_2', 'member_3', 'member_4'));

        // pop one member
        $membersBefore = $this->redisProxy->smembers('my_set_key');
        self::assertCount(4, $membersBefore);
        $member = $this->redisProxy->spop('my_set_key');
        $membersAfter = $this->redisProxy->smembers('my_set_key');
        self::assertCount(3, $membersAfter);
        self::assertEquals([$member], array_values(array_diff($membersBefore, $membersAfter)));
        self::assertEquals(3, $this->redisProxy->scard('my_set_key'));

        // pop more members
        $membersBefore = $this->redisProxy->smembers('my_set_key');
        self::assertCount(3, $membersBefore);
        $members = $this->redisProxy->spop('my_set_key', 2);
        $membersAfter = $this->redisProxy->smembers('my_set_key');
        self::assertCount(1, $membersAfter);
        sort($members);
        $membersDiff = array_values(array_diff($membersBefore, $membersAfter));
        sort($membersDiff);
        self::assertEquals($members, $membersDiff);
        self::assertEquals(1, $this->redisProxy->scard('my_set_key'));

        // pop 3 members from set with one member
        $membersBefore = $this->redisProxy->smembers('my_set_key');
        self::assertCount(1, $membersBefore);
        $members = $this->redisProxy->spop('my_set_key', 3);
        $membersAfter = $this->redisProxy->smembers('my_set_key');
        self::assertCount(0, $membersAfter);
        sort($members);
        $membersDiff = array_values(array_diff($membersBefore, $membersAfter));
        sort($membersDiff);
        self::assertEquals($members, $membersDiff);
        self::assertEquals(0, $this->redisProxy->scard('my_set_key'));
    }

    public function testSscan(): void
    {
        self::assertEquals(0, $this->redisProxy->scard('my_set_key'));
        $members = [];
        for ($i = 0; $i < 1000; ++$i) {
            $members[] = "member_$i";
        }
        self::assertEquals(1000, $this->redisProxy->sadd('my_set_key', $members));
        self::assertEquals(1000, $this->redisProxy->scard('my_set_key'));

        $count = 0;
        $iterator = null;
        while ($sscanMembers = $this->redisProxy->sscan('my_set_key', $iterator, null, 100)) {
            $count += count($sscanMembers);
            foreach ($sscanMembers as $sscanMember) {
                self::assertTrue(strpos($sscanMember, 'member_') === 0);
            }
        }
        self::assertEquals(1000, $count);
        self::assertEquals(0, $iterator);

        $count = 0;
        $iterator = null;
        while ($sscanMembers = $this->redisProxy->sscan('my_set_key', $iterator, 'member_1*', 100)) {
            $count += count($sscanMembers);
            foreach ($sscanMembers as $sscanMember) {
                self::assertTrue(strpos($sscanMember, 'member_1') === 0);
            }
        }
        self::assertEquals(111, $count);
        self::assertEquals(0, $iterator);

        $count = 0;
        $iterator = null;
        while ($sscanMembers = $this->redisProxy->sscan('my_set_key', $iterator, '*1*', 100)) {
            $count += count($sscanMembers);
            foreach ($sscanMembers as $sscanMember) {
                self::assertTrue(strpos($sscanMember, '1') !== false);
            }
        }
        self::assertEquals(271, $count);
        self::assertEquals(0, $iterator);
    }

    public function testLlen(): void
    {
        self::assertEquals(0, $this->redisProxy->llen('my_list_key'));
        self::assertEquals(1, $this->redisProxy->lpush('my_list_key', 'my_value'));
        self::assertEquals(1, $this->redisProxy->llen('my_list_key'));
    }

    public function testLpush(): void
    {
        // add one element
        self::assertEquals(1, $this->redisProxy->lpush('my_list_key', 'element'));
        self::assertEquals(1, $this->redisProxy->llen('my_list_key'));

        // add three elements
        self::assertEquals(4, $this->redisProxy->lpush('my_list_key', 'element_2', 'element_3', 'element_4'));
        self::assertEquals(4, $this->redisProxy->llen('my_list_key'));

        // add elements which are already in list
        self::assertEquals(7, $this->redisProxy->lpush('my_list_key', 'element_2', 'element_3', 'element_4'));
        self::assertEquals(7, $this->redisProxy->llen('my_list_key'));

        // add some new elements and some elements which are already in list
        self::assertEquals(11, $this->redisProxy->lpush('my_list_key', 'element_2', 'element_3', 'element_5', 'element_6'));
        self::assertEquals(11, $this->redisProxy->llen('my_list_key'));

        // add three elements as array
        self::assertEquals(14, $this->redisProxy->lpush('my_list_key', ['element_7', 'element_8', 'element_9']));
        self::assertEquals(14, $this->redisProxy->llen('my_list_key'));
    }

    public function testRpush(): void
    {
        // add one element
        self::assertEquals(1, $this->redisProxy->rpush('my_list_key', 'element'));
        self::assertEquals(1, $this->redisProxy->llen('my_list_key'));

        // add three elements
        self::assertEquals(4, $this->redisProxy->rpush('my_list_key', 'element_2', 'element_3', 'element_4'));
        self::assertEquals(4, $this->redisProxy->llen('my_list_key'));

        // add elements which are already in list
        self::assertEquals(7, $this->redisProxy->rpush('my_list_key', 'element_2', 'element_3', 'element_4'));
        self::assertEquals(7, $this->redisProxy->llen('my_list_key'));

        // add some new elements and some elements which are already in list
        self::assertEquals(11, $this->redisProxy->rpush('my_list_key', 'element_2', 'element_3', 'element_5', 'element_6'));
        self::assertEquals(11, $this->redisProxy->llen('my_list_key'));

        // add three elements as array
        self::assertEquals(14, $this->redisProxy->rpush('my_list_key', ['element_7', 'element_8', 'element_9']));
        self::assertEquals(14, $this->redisProxy->llen('my_list_key'));
    }

    public function testLrange(): void
    {
        self::assertEquals(0, $this->redisProxy->llen('my_list_key'));
        self::assertCount(0, $this->redisProxy->lrange('my_list_key', 0, -1));
        self::assertCount(0, $this->redisProxy->lrange('my_list_key', 0, 2));

        self::assertEquals(2, $this->redisProxy->lpush('my_list_key', 'element_1', 'element_2'));
        self::assertEquals(2, $this->redisProxy->llen('my_list_key'));
        self::assertCount(2, $this->redisProxy->lrange('my_list_key', 0, -1));
        self::assertCount(2, $this->redisProxy->lrange('my_list_key', 0, 2));
        self::assertEquals(['element_2', 'element_1'], $this->redisProxy->lrange('my_list_key', 0, -1));

        self::assertEquals(4, $this->redisProxy->rpush('my_list_key', 'element_3', 'element_4'));
        self::assertEquals(4, $this->redisProxy->llen('my_list_key'));
        self::assertCount(4, $this->redisProxy->lrange('my_list_key', 0, -1));
        self::assertCount(3, $this->redisProxy->lrange('my_list_key', 0, 2));
        self::assertEquals(['element_2', 'element_1', 'element_3', 'element_4'], $this->redisProxy->lrange('my_list_key', 0, -1));
    }

    public function testLindex(): void
    {
        self::assertEquals(0, $this->redisProxy->llen('my_list_key'));
        self::assertNull($this->redisProxy->lindex('my_list_key', 0));

        self::assertEquals(2, $this->redisProxy->lpush('my_list_key', 'element_1', 'element_2'));
        self::assertEquals(2, $this->redisProxy->llen('my_list_key'));
        self::assertEquals('element_2', $this->redisProxy->lindex('my_list_key', 0));
        self::assertEquals('element_1', $this->redisProxy->lindex('my_list_key', 1));
        self::assertNull($this->redisProxy->lindex('my_list_key', 2));

        self::assertEquals(4, $this->redisProxy->rpush('my_list_key', 'element_3', 'element_4'));
        self::assertEquals(4, $this->redisProxy->llen('my_list_key'));
        self::assertEquals('element_2', $this->redisProxy->lindex('my_list_key', 0));
        self::assertEquals('element_1', $this->redisProxy->lindex('my_list_key', 1));
        self::assertEquals('element_3', $this->redisProxy->lindex('my_list_key', 2));
        self::assertEquals('element_4', $this->redisProxy->lindex('my_list_key', 3));
        self::assertNull($this->redisProxy->lindex('my_list_key', 4));
    }

    public function testLpop(): void
    {
        self::assertEquals(0, $this->redisProxy->llen('my_list_key'));
        self::assertNull($this->redisProxy->lindex('my_list_key', 0));
        self::assertNull($this->redisProxy->lpop('my_list_key'));
        self::assertEquals(2, $this->redisProxy->lpush('my_list_key', 'element_1', 'element_2'));
        self::assertEquals(2, $this->redisProxy->llen('my_list_key'));
        self::assertEquals('element_2', $this->redisProxy->lindex('my_list_key', 0));
        self::assertEquals('element_1', $this->redisProxy->lindex('my_list_key', 1));
        self::assertNull($this->redisProxy->lindex('my_list_key', 2));
        self::assertEquals('element_2', $this->redisProxy->lpop('my_list_key'));
        self::assertEquals(1, $this->redisProxy->llen('my_list_key'));
        self::assertEquals('element_1', $this->redisProxy->lindex('my_list_key', 0));
        self::assertNull($this->redisProxy->lindex('my_list_key', 1));
        self::assertEquals('element_1', $this->redisProxy->lpop('my_list_key'));
        self::assertEquals(0, $this->redisProxy->llen('my_list_key'));
        self::assertNull($this->redisProxy->lindex('my_list_key', 0));
    }

    public function testRpop(): void
    {
        self::assertEquals(0, $this->redisProxy->llen('my_list_key'));
        self::assertNull($this->redisProxy->lindex('my_list_key', 0));
        self::assertNull($this->redisProxy->rpop('my_list_key'));
        self::assertEquals(2, $this->redisProxy->lpush('my_list_key', 'element_1', 'element_2'));
        self::assertEquals(2, $this->redisProxy->llen('my_list_key'));
        self::assertEquals('element_2', $this->redisProxy->lindex('my_list_key', 0));
        self::assertEquals('element_1', $this->redisProxy->lindex('my_list_key', 1));
        self::assertNull($this->redisProxy->lindex('my_list_key', 2));
        self::assertEquals('element_1', $this->redisProxy->rpop('my_list_key'));
        self::assertEquals(1, $this->redisProxy->llen('my_list_key'));
        self::assertEquals('element_2', $this->redisProxy->lindex('my_list_key', 0));
        self::assertNull($this->redisProxy->lindex('my_list_key', 1));
        self::assertEquals('element_2', $this->redisProxy->rpop('my_list_key'));
        self::assertEquals(0, $this->redisProxy->llen('my_list_key'));
        self::assertNull($this->redisProxy->lindex('my_list_key', 0));
    }

    public function testZcard(): void
    {
        self::assertEquals(0, $this->redisProxy->zcard('my_sorted_set_key'));
        self::assertEquals(1, $this->redisProxy->zadd('my_sorted_set_key', 1, 'my_member'));
        self::assertEquals(1, $this->redisProxy->zcard('my_sorted_set_key'));
    }

    public function testZadd(): void
    {
        self::assertEquals(0, $this->redisProxy->zcard('my_sorted_set_key'));
        self::assertEquals(1, $this->redisProxy->zadd('my_sorted_set_key', 1, 'my_member'));
        self::assertEquals(1, $this->redisProxy->zcard('my_sorted_set_key'));

        self::assertEquals(2, $this->redisProxy->zadd('my_sorted_set_key', 2, 'my_member_2', 3, 'my_member_3'));
        self::assertEquals(3, $this->redisProxy->zcard('my_sorted_set_key'));

        self::assertEquals(0, $this->redisProxy->zadd('my_sorted_set_key', 10, 'my_member'));
        self::assertEquals(3, $this->redisProxy->zcard('my_sorted_set_key'));

        self::assertEquals(1, $this->redisProxy->zadd('my_sorted_set_key', 2, 'my_member_2', 3, 'my_member_3', 4, 'my_member_4'));
        self::assertEquals(4, $this->redisProxy->zcard('my_sorted_set_key'));

        self::assertEquals(3, $this->redisProxy->zadd('my_sorted_set_key', ['my_member_5' => 5, 'my_member_6' => 4, 'my_member_7' => 3]));
        self::assertEquals(7, $this->redisProxy->zcard('my_sorted_set_key'));
    }

    public function testZrange(): void
    {
        self::assertEquals(0, $this->redisProxy->zcard('my_sorted_set_key'));
        self::assertCount(0, $this->redisProxy->zrange('my_sorted_set_key', 0, -1));
        self::assertCount(0, $this->redisProxy->zrange('my_sorted_set_key', 0, 2));

        self::assertEquals(2, $this->redisProxy->zadd('my_sorted_set_key', 1, 'element_1', 2, 'element_2'));
        self::assertEquals(2, $this->redisProxy->zcard('my_sorted_set_key'));
        self::assertCount(2, $this->redisProxy->zrange('my_sorted_set_key', 0, -1));
        self::assertCount(2, $this->redisProxy->zrange('my_sorted_set_key', 0, 2));
        self::assertEquals(['element_1', 'element_2'], $this->redisProxy->zrange('my_sorted_set_key', 0, -1));
        self::assertEquals(['element_1' => 1, 'element_2' => 2], $this->redisProxy->zrange('my_sorted_set_key', 0, -1, true));

        self::assertEquals(2, $this->redisProxy->zadd('my_sorted_set_key', 3, 'element_3', 4, 'element_4'));
        self::assertEquals(4, $this->redisProxy->zcard('my_sorted_set_key'));
        self::assertCount(4, $this->redisProxy->zrange('my_sorted_set_key', 0, -1));
        self::assertCount(3, $this->redisProxy->zrange('my_sorted_set_key', 0, 2));
        self::assertEquals(['element_1', 'element_2', 'element_3', 'element_4'], $this->redisProxy->zrange('my_sorted_set_key', 0, -1));
        self::assertEquals(['element_1' => 1, 'element_2' => 2, 'element_3' => 3, 'element_4' => 4], $this->redisProxy->zrange('my_sorted_set_key', 0, -1, true));

        self::assertEquals(2, $this->redisProxy->zadd('my_sorted_set_key', ['element_5' => -5, 'element_6' => -6]));
        self::assertEquals(6, $this->redisProxy->zcard('my_sorted_set_key'));
        self::assertCount(6, $this->redisProxy->zrange('my_sorted_set_key', 0, -1));
        self::assertCount(3, $this->redisProxy->zrange('my_sorted_set_key', 0, 2));
        self::assertEquals(['element_6', 'element_5', 'element_1', 'element_2', 'element_3', 'element_4'], $this->redisProxy->zrange('my_sorted_set_key', 0, -1));
        self::assertEquals(['element_6' => -6, 'element_5' => -5, 'element_1' => 1, 'element_2' => 2, 'element_3' => 3, 'element_4' => 4], $this->redisProxy->zrange('my_sorted_set_key', 0, -1, true));

        self::assertEquals(2, $this->redisProxy->zadd('my_sorted_set_key', ['element_7' => -5, 'element_8' => -6]));
        self::assertEquals(8, $this->redisProxy->zcard('my_sorted_set_key'));
        self::assertCount(8, $this->redisProxy->zrange('my_sorted_set_key', 0, -1));
        self::assertCount(3, $this->redisProxy->zrange('my_sorted_set_key', 0, 2));
        self::assertEquals(['element_6', 'element_8', 'element_5', 'element_7', 'element_1', 'element_2', 'element_3', 'element_4'], $this->redisProxy->zrange('my_sorted_set_key', 0, -1));
        self::assertEquals(['element_6' => -6, 'element_8' => -6, 'element_5' => -5, 'element_7' => -5, 'element_1' => 1, 'element_2' => 2, 'element_3' => 3, 'element_4' => 4], $this->redisProxy->zrange('my_sorted_set_key', 0, -1, true));
    }

    public function testZrevrange(): void
    {
        self::assertEquals(0, $this->redisProxy->zcard('my_sorted_set_key'));
        self::assertCount(0, $this->redisProxy->zrevrange('my_sorted_set_key', 0, -1));
        self::assertCount(0, $this->redisProxy->zrevrange('my_sorted_set_key', 0, 2));

        self::assertEquals(2, $this->redisProxy->zadd('my_sorted_set_key', 1, 'element_1', 2, 'element_2'));
        self::assertEquals(2, $this->redisProxy->zcard('my_sorted_set_key'));
        self::assertCount(2, $this->redisProxy->zrevrange('my_sorted_set_key', 0, -1));
        self::assertCount(2, $this->redisProxy->zrevrange('my_sorted_set_key', 0, 2));
        self::assertEquals(['element_2', 'element_1'], $this->redisProxy->zrevrange('my_sorted_set_key', 0, -1));
        self::assertEquals(['element_2' => 2, 'element_1' => 1], $this->redisProxy->zrevrange('my_sorted_set_key', 0, -1, true));

        self::assertEquals(2, $this->redisProxy->zadd('my_sorted_set_key', 3, 'element_3', 4, 'element_4'));
        self::assertEquals(4, $this->redisProxy->zcard('my_sorted_set_key'));
        self::assertCount(4, $this->redisProxy->zrevrange('my_sorted_set_key', 0, -1));
        self::assertCount(3, $this->redisProxy->zrevrange('my_sorted_set_key', 0, 2));
        self::assertEquals(['element_4', 'element_3', 'element_2', 'element_1'], $this->redisProxy->zrevrange('my_sorted_set_key', 0, -1));
        self::assertEquals(['element_4' => 4, 'element_3' => 3, 'element_2' => 2, 'element_1' => 1], $this->redisProxy->zrevrange('my_sorted_set_key', 0, -1, true));

        self::assertEquals(2, $this->redisProxy->zadd('my_sorted_set_key', ['element_5' => -5, 'element_6' => -6]));
        self::assertEquals(6, $this->redisProxy->zcard('my_sorted_set_key'));
        self::assertCount(6, $this->redisProxy->zrevrange('my_sorted_set_key', 0, -1));
        self::assertCount(3, $this->redisProxy->zrevrange('my_sorted_set_key', 0, 2));
        self::assertEquals(['element_4', 'element_3', 'element_2', 'element_1', 'element_5', 'element_6'], $this->redisProxy->zrevrange('my_sorted_set_key', 0, -1));
        self::assertEquals(['element_4' => 4, 'element_3' => 3, 'element_2' => 2, 'element_1' => 1, 'element_5' => -5, 'element_6' => -6], $this->redisProxy->zrevrange('my_sorted_set_key', 0, -1, true));

        self::assertEquals(2, $this->redisProxy->zadd('my_sorted_set_key', ['element_7' => -5, 'element_8' => -6]));
        self::assertEquals(8, $this->redisProxy->zcard('my_sorted_set_key'));
        self::assertCount(8, $this->redisProxy->zrevrange('my_sorted_set_key', 0, -1));
        self::assertCount(3, $this->redisProxy->zrevrange('my_sorted_set_key', 0, 2));
        self::assertEquals(['element_4', 'element_3', 'element_2', 'element_1', 'element_7', 'element_5', 'element_8', 'element_6'], $this->redisProxy->zrevrange('my_sorted_set_key', 0, -1));
        self::assertEquals(['element_4' => 4, 'element_3' => 3, 'element_2' => 2, 'element_1' => 1, 'element_7' => -5, 'element_5' => -5, 'element_8' => -6, 'element_6' => -6], $this->redisProxy->zrevrange('my_sorted_set_key', 0, -1, true));
    }

    public function testZrangebyscore(): void
    {
        self::assertEquals(0, $this->redisProxy->zcard('my_sorted_set_key'));
        self::assertEquals([], $this->redisProxy->zrangebyscore('my_sorted_set_key', '-inf', 'inf'));
        self::assertEquals([], $this->redisProxy->zrangebyscore('my_sorted_set_key', '-inf', 0));
        self::assertEquals([], $this->redisProxy->zrangebyscore('my_sorted_set_key', 0, 'inf'));

        self::assertEquals(3, $this->redisProxy->zadd('my_sorted_set_key', -1, 'element_1', 0, 'element_2', 1, 'element_3'));
        self::assertEquals(3, $this->redisProxy->zcard('my_sorted_set_key'));

        self::assertEquals(['element_1', 'element_2', 'element_3'], $this->redisProxy->zrangebyscore('my_sorted_set_key', '-inf', 'inf'));
        self::assertEquals(['element_2', 'element_3'], $this->redisProxy->zrangebyscore('my_sorted_set_key', '-inf', 'inf', ['limit' => [1, 2]]));
        self::assertEquals(['element_1', 'element_2'], $this->redisProxy->zrangebyscore('my_sorted_set_key', '-inf', 0));
        self::assertEquals(['element_2', 'element_3'], $this->redisProxy->zrangebyscore('my_sorted_set_key', 0, 'inf'));

        self::assertEquals(['element_1' => -1, 'element_2' => 0, 'element_3' => 1], $this->redisProxy->zrangebyscore('my_sorted_set_key', '-inf', 'inf', ['withscores' => true]));
        self::assertEquals(['element_1' => -1, 'element_2' => 0], $this->redisProxy->zrangebyscore('my_sorted_set_key', '-inf', 0, ['withscores' => true]));
        self::assertEquals(['element_2' => 0, 'element_3' => 1], $this->redisProxy->zrangebyscore('my_sorted_set_key', 0, 'inf', ['withscores' => true]));
    }

    public function testZscan(): void
    {
        self::assertEquals(0, $this->redisProxy->zcard('my_sorted_set_key'));

        $members = [];
        for ($i = 0; $i < 1000; ++$i) {
            $members["member_$i"] = mt_rand(0, 1000);
        }
        self::assertEquals(1000, $this->redisProxy->zadd('my_sorted_set_key', $members));
        self::assertEquals(1000, $this->redisProxy->zcard('my_sorted_set_key'));

        $count = 0;
        $iterator = null;
        while ($zscanMembers = $this->redisProxy->zscan('my_sorted_set_key', $iterator, null, 100)) {
            $count += count($zscanMembers);
            foreach ($zscanMembers as $zscanMember => $score) {
                self::assertTrue(strpos($zscanMember, 'member_') === 0);
            }
        }
        self::assertEquals(1000, $count);
        self::assertEquals(0, $iterator);

        $count = 0;
        $iterator = null;
        while ($zscanMembers = $this->redisProxy->zscan('my_sorted_set_key', $iterator, 'member_1*', 100)) {
            $count += count($zscanMembers);
            foreach ($zscanMembers as $zscanMember => $score) {
                self::assertTrue(strpos($zscanMember, 'member_1') === 0);
            }
        }
        self::assertEquals(111, $count);
        self::assertEquals(0, $iterator);

        $count = 0;
        $iterator = null;
        $maxScore = 0;
        while ($zscanMembers = $this->redisProxy->zscan('my_sorted_set_key', $iterator, '*1*', 100)) {
            $count += count($zscanMembers);
            foreach ($zscanMembers as $zscanMember => $score) {
                self::assertTrue(strpos($zscanMember, '1') !== false);
            }
        }
        self::assertEquals(271, $count);
        self::assertEquals(0, $iterator);
    }

    public function testZrankAndZrevrank(): void
    {
        self::assertEquals(0, $this->redisProxy->zcard('my_sorted_set_key'));

        self::assertNull($this->redisProxy->zrank('my_sorted_set_key', 'something'));
        self::assertNull($this->redisProxy->zrevrank('my_sorted_set_key', 'something'));

        self::assertEquals(1, $this->redisProxy->zadd('my_sorted_set_key', 100, 'first'));
        self::assertEquals(1, $this->redisProxy->zadd('my_sorted_set_key', 200, 'second'));
        self::assertEquals(1, $this->redisProxy->zadd('my_sorted_set_key', 30, 'third'));

        self::assertEquals(0, $this->redisProxy->zrank('my_sorted_set_key', 'third'));
        self::assertEquals(2, $this->redisProxy->zrevrank('my_sorted_set_key', 'third'));

        self::assertNull($this->redisProxy->zrank('my_sorted_set_key', 'something'));
        self::assertNull($this->redisProxy->zrevrank('my_sorted_set_key', 'something'));
    }

    public function testZrem(): void
    {
        self::assertEquals(0, $this->redisProxy->zcard('my_sorted_set_key'));
        self::assertNull($this->redisProxy->zrank('my_sorted_set_key', 'first'));
        self::assertNull($this->redisProxy->zrank('my_sorted_set_key', 'second'));

        self::assertEquals(1, $this->redisProxy->zadd('my_sorted_set_key', 100, 'first'));
        self::assertEquals(1, $this->redisProxy->zadd('my_sorted_set_key', 200, 'second'));

        self::assertEquals(0, $this->redisProxy->zrank('my_sorted_set_key', 'first'));
        self::assertEquals(1, $this->redisProxy->zrank('my_sorted_set_key', 'second'));

        self::assertEquals(1, $this->redisProxy->zrem('my_sorted_set_key', 'first'));

        self::assertNull($this->redisProxy->zrank('my_sorted_set_key', 'first'));
        self::assertEquals(0, $this->redisProxy->zrank('my_sorted_set_key', 'second'));
        self::assertEquals(1, $this->redisProxy->zrem('my_sorted_set_key', 'second'));

        self::assertEquals(1, $this->redisProxy->zadd('my_sorted_set_key', 100, 'first'));
        self::assertEquals(1, $this->redisProxy->zadd('my_sorted_set_key', 200, 'second'));
        self::assertEquals(2, $this->redisProxy->zrem('my_sorted_set_key', 'first', 'second'));

        self::assertEquals(1, $this->redisProxy->zadd('my_sorted_set_key', 100, 'first'));
        self::assertEquals(1, $this->redisProxy->zadd('my_sorted_set_key', 200, 'second'));
        self::assertEquals(2, $this->redisProxy->zrem('my_sorted_set_key', ['first', 'second']));
    }

    public function testType(): void
    {
        self::assertNull($this->redisProxy->type('my_key'));
        $this->redisProxy->set('my_key', 'my_value');
        self::assertEquals(RedisProxy::TYPE_STRING, $this->redisProxy->type('my_key'));

        self::assertNull($this->redisProxy->type('my_set_key'));
        $this->redisProxy->sadd('my_set_key', 'my_member');
        self::assertEquals(RedisProxy::TYPE_SET, $this->redisProxy->type('my_set_key'));

        self::assertNull($this->redisProxy->type('my_hash_key'));
        $this->redisProxy->hset('my_hash_key', 'my_key', 'my_value');
        self::assertEquals(RedisProxy::TYPE_HASH, $this->redisProxy->type('my_hash_key'));

        self::assertNull($this->redisProxy->type('my_list_key'));
        $this->redisProxy->lpush('my_list_key', 'my_value');
        self::assertEquals(RedisProxy::TYPE_LIST, $this->redisProxy->type('my_list_key'));

        self::assertNull($this->redisProxy->type('my_sorted_set_key'));
        $this->redisProxy->zadd('my_sorted_set_key', 1, 'my_value');
        self::assertEquals(RedisProxy::TYPE_SORTED_SET, $this->redisProxy->type('my_sorted_set_key'));
    }
}
