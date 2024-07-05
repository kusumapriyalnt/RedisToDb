package com.lnt.ptd.redisToDB;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.IOException;
import java.util.List;

class ServerConfig {
    // fields
    private final String name;
    private final List<String> redisTopics;
    private final String mysqlIpAddress;
    private final String mysqlDb;
    private final String mysqlUserName;
    private final String mysqlPassword;
    private final JedisPool jedisPool;
    private final MessageProcessor messageProcessor;

    // constructor
    public ServerConfig(String name, String redisIp, int redisPort, List<String> redisTopics, String mysqlIpAddress, String mysqlDb, String mysqlUserName, String mysqlPassword) throws IOException {
        this.name = name;
        this.redisTopics = redisTopics;
        this.mysqlIpAddress = mysqlIpAddress;
        this.mysqlDb = mysqlDb;
        this.mysqlUserName = mysqlUserName;
        this.mysqlPassword = mysqlPassword;
        this.jedisPool = new JedisPool(new JedisPoolConfig(), redisIp, redisPort);
        this.messageProcessor = new MessageProcessor(this);
    }

    // getters and setters
    public String getName() {
        return name;
    }

    public List<String> getRedisTopics() {
        return redisTopics;
    }

    public String getMysqlIpAddress() {
        return mysqlIpAddress;
    }

    public String getMysqlDb() {
        return mysqlDb;
    }

    public String getMysqlUserName() {
        return mysqlUserName;
    }

    public String getMysqlPassword() {
        return mysqlPassword;
    }

    public JedisPool getJedisPool() {
        return jedisPool;
    }

    // method to initialise and start the message processor
    public void startMessageProcessor() {
        // assigning a new thread
        Thread messageProcessorThread = new Thread(messageProcessor);
        // starting the thread
        messageProcessorThread.start();
    }

    // method to stop the data processor
    public void stopMessageProcessor() {
        messageProcessor.stop();
    }

}
