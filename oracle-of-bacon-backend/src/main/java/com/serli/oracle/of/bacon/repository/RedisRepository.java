package com.serli.oracle.of.bacon.repository;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public class RedisRepository {
    private JedisPool pool;

    public RedisRepository(){
        URI uri = URI.create("redis://thomas:redisRuddle@redis-16244.c3.eu-west-1-2.ec2.cloud.redislabs.com:16244");
        pool = new JedisPool(new JedisPoolConfig(),uri);
    }

    public List<String> getLastTenSearches() {
        Jedis jedis = pool.getResource();

        List<String> topten = jedis.lrange("topten",0,9);
        if(topten == null)
            topten = new ArrayList<String>();
        return topten;
    }

    public void push(String actorname) {
        Jedis jedis = pool.getResource();

        jedis.lpush("topten", actorname);

        if (jedis.llen("topten") > 10) {
            jedis.rpop("topten");
        }
    }
}
