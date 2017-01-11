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

    public function testSelect()
    {
        self::assertTrue($this->redisProxy->select(1));
        self::assertTrue($this->redisProxy->select(0));
    }

    /**
     * @expectedException \RedisProxy\RedisProxyException
     * @expectedExceptionMessage Invalid DB index
     */
    public function testSelectInvalidDatabase()
    {
        $this->redisProxy->select(-1);
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

        $serverInfo = $this->redisProxy->info('server');
        self::assertNotEmpty($serverInfo);
    }

    /**
     * @expectedException \RedisProxy\RedisProxyException
     * @expectedExceptionMessage Info section "unknown" doesn't exist
     */
    public function testUnknownInfoSection()
    {
        $this->redisProxy->info('unknown');
    }

    public function testSetGet()
    {
        self::assertNull($this->redisProxy->get('my_key'));
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
        self::assertNull($this->redisProxy->get('my_key'));

        self::assertEquals(0, $this->redisProxy->del('my_key'));
        self::assertTrue($this->redisProxy->set('my_key', 'my_value'));
        self::assertEquals(1, $this->redisProxy->del('my_key'));
        self::assertNull($this->redisProxy->get('my_key'));
    }

    public function testMultipleDelete()
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

    public function testHset()
    {
        self::assertNull($this->redisProxy->hget('my_hash_key', 'my_field'));
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

    public function testSadd()
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

    public function testSpop()
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

    public function testSscan()
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
                self::assertTrue(strpos($sscanMember, 'member_') === 0);
            }
        }
        self::assertEquals(111, $count);
        self::assertEquals(0, $iterator);

        $count = 0;
        $iterator = null;
        $res = [];
        while ($sscanMembers = $this->redisProxy->sscan('my_set_key', $iterator, '*1*', 100)) {
            $res = array_merge($res, $sscanMembers);
            $count += count($sscanMembers);
            foreach ($sscanMembers as $sscanMember) {
                self::assertTrue(strpos($sscanMember, 'member_') === 0);
            }
        }
        self::assertEquals(271, $count);
        self::assertEquals(0, $iterator);
    }
}
