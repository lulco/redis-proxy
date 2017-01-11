<?php

namespace RedisProxy\Tests;

use PHPUnit_Framework_TestCase;
use RedisProxy\RedisProxy;

abstract class BaseDriverTest extends PHPUnit_Framework_TestCase
{
    /** @var RedisProxy */
    protected $redisProxy;

    protected function setUp()
    {
        $this->initializeDriver();
    }

    /**
     * @return RedisProxy
     */
    abstract protected function initializeDriver();

    /**
     * @expectedException \RedisProxy\RedisProxyException
     * @expectedExceptionMessage Invalid DB index
     */
    public function testSelect()
    {
        self::assertTrue($this->redisProxy->select(1));
        self::assertTrue($this->redisProxy->select(0));
        self::assertFalse($this->redisProxy->select(-1));
    }

    public function testInfo()
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

        $keyspaceInfo = $this->redisProxy->info('keyspace');
        self::assertNotEmpty($keyspaceInfo);
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
        $this->redisProxy->select(0);
        $this->redisProxy->set('first_key', 'first_value');

        $this->redisProxy->select(1);
        $this->redisProxy->set('second_key', 'second_value');
        $this->redisProxy->set('third_key', 'third_value');

        $keyspaceInfo = $this->redisProxy->info('keyspace');
        self::assertNotEmpty($keyspaceInfo);
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
    }

    public function testSetGet()
    {
        self::assertFalse($this->redisProxy->get('my_key'));
        self::assertTrue($this->redisProxy->set('my_key', 'my_value'));
        self::assertEquals('my_value', $this->redisProxy->get('my_key'));
        self::assertTrue($this->redisProxy->set('my_key', 'my_new_value'));
        self::assertEquals('my_new_value', $this->redisProxy->get('my_key'));
    }

    public function testMget()
    {
        self::assertEquals([false, false], $this->redisProxy->mget(['first_key', 'second_key']));
        self::assertTrue($this->redisProxy->set('first_key', 'first_value'));
        self::assertTrue($this->redisProxy->set('second_key', 'second_value'));
        self::assertEquals(['first_value', 'second_value'], $this->redisProxy->mget(['first_key', 'second_key']));
        self::assertEquals(['first_value', 'second_value', false], $this->redisProxy->mget(['first_key', 'second_key', 'third_key']));
        self::assertTrue($this->redisProxy->set('third_key', 'third_value'));
        self::assertEquals(['first_value', 'second_value', 'third_value'], $this->redisProxy->mget(['first_key', 'second_key', 'third_key']));
    }

    public function testDelete()
    {
        self::assertEquals(0, $this->redisProxy->delete('my_key'));
        self::assertTrue($this->redisProxy->set('my_key', 'my_value'));
        self::assertEquals(1, $this->redisProxy->delete('my_key'));
        self::assertFalse($this->redisProxy->get('my_key'));

        self::assertEquals(0, $this->redisProxy->del('my_key'));
        self::assertTrue($this->redisProxy->set('my_key', 'my_value'));
        self::assertEquals(1, $this->redisProxy->del('my_key'));
        self::assertFalse($this->redisProxy->get('my_key'));
    }

    public function testMultipleDelete()
    {
        self::assertEquals(0, $this->redisProxy->delete('first_key', 'second_key', 'third_key'));
        self::assertFalse($this->redisProxy->get('first_key'));
        self::assertFalse($this->redisProxy->get('second_key'));
        self::assertFalse($this->redisProxy->get('third_key'));

        self::assertTrue($this->redisProxy->set('first_key', 'first_value'));
        self::assertTrue($this->redisProxy->set('second_key', 'second_value'));
        self::assertTrue($this->redisProxy->set('third_key', 'third_value'));
        self::assertEquals(3, $this->redisProxy->delete('first_key', 'second_key', 'third_key'));
        self::assertFalse($this->redisProxy->get('first_key'));
        self::assertFalse($this->redisProxy->get('second_key'));
        self::assertFalse($this->redisProxy->get('third_key'));

        self::assertTrue($this->redisProxy->set('second_key', 'second_value'));
        self::assertEquals(1, $this->redisProxy->delete(['first_key', 'second_key', 'third_key']));
        self::assertFalse($this->redisProxy->get('first_key'));
        self::assertFalse($this->redisProxy->get('second_key'));
        self::assertFalse($this->redisProxy->get('third_key'));
    }

    public function testHset()
    {
        self::assertFalse($this->redisProxy->hget('my_hash_key', 'my_field'));
        self::assertEquals(1, $this->redisProxy->hset('my_hash_key', 'my_field', 'my_value'));
        self::assertEquals('my_value', $this->redisProxy->hget('my_hash_key', 'my_field'));
        self::assertEquals(0, $this->redisProxy->hset('my_hash_key', 'my_field', 'my_new_value'));
        self::assertEquals('my_new_value', $this->redisProxy->hget('my_hash_key', 'my_field'));
    }

    public function testHgetAll()
    {
        self::assertTrue(is_array($this->redisProxy->hgetall('my_hash_key')));
        self::assertCount(0, $this->redisProxy->hgetall('my_hash_key'));
        self::assertEquals(1, $this->redisProxy->hset('my_hash_key', 'my_first_field', 'my_first_value'));
        self::assertEquals(1, $this->redisProxy->hset('my_hash_key', 'my_second_field', 'my_second_value'));
        self::assertTrue(is_array($this->redisProxy->hgetall('my_hash_key')));
        self::assertCount(2, $this->redisProxy->hgetall('my_hash_key'));
        self::assertArrayHasKey('my_first_field', $this->redisProxy->hgetall('my_hash_key'));
        self::assertArrayHasKey('my_second_field', $this->redisProxy->hGetAll('my_hash_key'));
        self::assertArrayNotHasKey('my_third_field', $this->redisProxy->hgetall('my_hash_key'));
    }

    public function testHlen()
    {
        self::assertEquals(0, $this->redisProxy->hlen('my_hash_key'));
        self::assertEquals(1, $this->redisProxy->hset('my_hash_key', 'my_first_field', 'my_first_value'));
        self::assertEquals(1, $this->redisProxy->hlen('my_hash_key'));
        self::assertEquals(1, $this->redisProxy->hset('my_hash_key', 'my_second_field', 'my_second_value'));
        self::assertEquals(2, $this->redisProxy->hlen('my_hash_key'));
        self::assertEquals(1, $this->redisProxy->hdel('my_hash_key', 'my_first_field'));
        self::assertEquals(1, $this->redisProxy->hLen('my_hash_key'));
    }

    public function testHdel()
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
}
