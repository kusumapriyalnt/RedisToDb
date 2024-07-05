package com.lnt.ptd.redisToDB;

import com.google.gson.*;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.time.LocalDateTime;
import java.util.*;


public class Main {
    //List of server configs to store the configuration of redis and respective mysql
    private static final List<ServerConfig> serverConfigs = new ArrayList<>();
    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args){
        LOGGER.info("Starting the program at "+ LocalDateTime.now());
        // Load method to load the configuration
        loadConfiguration();
        LOGGER.info("Loaded the properties from the config file");
        // Iterating through server configs and initialising message processor thread
        for (ServerConfig serverConfig : serverConfigs) {
            serverConfig.startMessageProcessor();
            LOGGER.info("Thread started for "+ serverConfig.getName());
        }
        // code for shut down of program
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            for (ServerConfig serverConfig : serverConfigs) {
                // stopping message processor
                serverConfig.stopMessageProcessor();
                // closing connections of Redis
                serverConfig.getJedisPool().close();
                LOGGER.info("Thread stopped for "+ serverConfig.getName());
            }
            LOGGER.info("Application stopped gracefully!!");
        }));
    }
    // Function to load configuration from the config file
    private static void loadConfiguration() {
        try (FileReader reader = new FileReader("../config/config.json")) {
            // Reading the file and parsing the content in it
            JsonElement jsonElement = JsonParser.parseReader(reader);
            JsonObject jsonObject = jsonElement.getAsJsonObject();
            JsonArray serversArray = jsonObject.getAsJsonArray("servers");

            // storing the details in server config objects
            for (JsonElement serverElement : serversArray) {
                JsonObject serverObject = serverElement.getAsJsonObject();
                String name = serverObject.get("name").getAsString();
                String redisIp = serverObject.get("redisIp").getAsString();
                int redisPort = serverObject.get("port").getAsInt();
                List<String> redisTopics = new ArrayList<>();
                for (JsonElement topic : serverObject.getAsJsonArray("topics")) {
                    redisTopics.add(topic.getAsString());
                }

                JsonObject mysqlConfig = serverObject.getAsJsonObject("mysql");
                String mysqlIpAddress = mysqlConfig.get("ipAddress").getAsString();
                String mysqlDb = mysqlConfig.get("db").getAsString();
                String mysqlUserName = mysqlConfig.get("userName").getAsString();
                String mysqlPassword = mysqlConfig.get("password").getAsString();

                ServerConfig serverConfig = new ServerConfig(name, redisIp, redisPort, redisTopics, mysqlIpAddress, mysqlDb, mysqlUserName, mysqlPassword);
                serverConfigs.add(serverConfig);
            }
        } catch (FileNotFoundException e) {
            // File not found exception
            LOGGER.error("Error loading configuration file: " + e.getMessage());
        } catch (IOException e) {
            // Any Run time exception
            throw new RuntimeException(e);
        }
    }
}



