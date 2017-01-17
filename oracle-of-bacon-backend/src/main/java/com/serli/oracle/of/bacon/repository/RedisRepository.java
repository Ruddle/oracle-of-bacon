package com.serli.oracle.of.bacon.repository;

import redis.clients.jedis.Jedis;
import java.util.ArrayList;
import java.util.List;

public class RedisRepository {
    private Jedis jedis;
    public RedisRepository(){
        jedis = new Jedis("redis://thomas:redisRuddle@redis-16244.c3.eu-west-1-2.ec2.cloud.redislabs.com:16244");
    }

    public List<String> getLastTenSearches() {
        List<String> topten = jedis.lrange("topten",0,9);
        if(topten == null)
            topten = new ArrayList<String>();
        return topten;
    }
}
