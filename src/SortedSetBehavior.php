<?php

namespace RedisProxy;

/**
 * @method int zcard(string $key) Get the number of members in a sorted set
 */
trait SortedSetBehavior
{
    use AbstractCommonBehavior;

    /**
     * Add one or more members to a sorted set, or update its score if it already exists
     * @param string $key
     * @param array $dictionary (score1, member1[, score2, member2]) or associative array: [member1 => score1, member2 => score2]
     * @return int
     */
    public function zadd($key, ...$dictionary)
    {
        $this->init();
        if (is_array($dictionary[0])) {
            $return = 0;
            foreach ($dictionary[0] as $member => $score) {
                $res = $this->zadd($key, $score, $member);
                $return += $res;
            }
            return $return;
        }
        return $this->driver->zadd($key, ...$dictionary);
    }

    /**
     * Return a range of members in a sorted set, by index
     * @param string $key
     * @param int $start
     * @param int $stop
     * @param boolean $withscores
     * @return array
     */
    public function zrange($key, $start, $stop, $withscores = false)
    {
        $this->init();
        if ($this->actualDriver() === self::DRIVER_PREDIS) {
            return $this->driver->zrange($key, $start, $stop, ['WITHSCORES' => $withscores]);
        }
        return $this->driver->zrange($key, $start, $stop, $withscores);
    }

    /**
     * Return a range of members in a sorted set, by index, with scores ordered from high to low
     * @param string $key
     * @param int $start
     * @param int $stop
     * @param boolean $withscores
     * @return array
     */
    public function zrevrange($key, $start, $stop, $withscores = false)
    {
        $this->init();
        if ($this->actualDriver() === self::DRIVER_PREDIS) {
            return $this->driver->zrevrange($key, $start, $stop, ['WITHSCORES' => $withscores]);
        }
        return $this->driver->zrevrange($key, $start, $stop, $withscores);
    }
}
