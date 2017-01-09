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

    public function testSet()
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
}
