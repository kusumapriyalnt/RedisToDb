package com.lnt.ptd.redisToDB;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;

import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.*;

class MessageProcessor implements Runnable {
    private final ServerConfig serverConfig;
    private volatile boolean running = true;
    private final LinkedBlockingQueue<String[]> analogQueue = new LinkedBlockingQueue<>();
    private final LinkedBlockingQueue<String[]> discreteQueue = new LinkedBlockingQueue<>();
    private final int BATCH_SIZE;
    private final int THREAD_POOL_SIZE;
    private static final int TOPIC_PER_THREAD = 100;
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static final Logger LOGGER = LoggerFactory.getLogger(MessageProcessor.class);
    private static Map<String, String> channelMap = new ConcurrentHashMap<>();

    public MessageProcessor(ServerConfig serverConfig) throws IOException {
        this.serverConfig = serverConfig;
        int size = serverConfig.getRedisTopics().size();
        if (size % TOPIC_PER_THREAD == 0) {
            this.THREAD_POOL_SIZE = size / TOPIC_PER_THREAD;
        } else {
            this.THREAD_POOL_SIZE = (size / TOPIC_PER_THREAD) + 1;
        }
        if (size <= 20) {
            BATCH_SIZE = 100;
        } else if (size <= 50) {
            BATCH_SIZE = 500;
        } else if (size <= 100) {
            BATCH_SIZE = 1000;
        } else {
            BATCH_SIZE = 2000;
        }
        LOGGER.info("The batch size for " + serverConfig.getName() + " is " + BATCH_SIZE);
        AssetMeasDBMaps.update(serverConfig.getMysqlDb(), serverConfig.getMysqlIpAddress(), serverConfig.getMysqlUserName(), serverConfig.getMysqlPassword());
        LOGGER.info("AssetMeasDBMaps are updated successfully");
        readChannelConfig();
        LOGGER.info("Read the configuration of channel names and related Asset Id's");
    }

    public void readChannelConfig() throws IOException {
        Gson gson = new Gson();
        try (FileReader reader = new FileReader("../config/redisTopicConfig.json")) {
            JsonReader jsonReader = new JsonReader(reader);
            channelMap = gson.fromJson(jsonReader, new TypeToken<Map<String, String>>() {}.getType());
        }
        LOGGER.info("Updated the Channel Map");
    }

    public void start() {
        ExecutorService executor = Executors.newFixedThreadPool(THREAD_POOL_SIZE * 2);
        for (int i = 0; i < THREAD_POOL_SIZE; i++) {
            executor.submit(() -> processQueue(analogQueue, "lntds_analog_data"));
            executor.submit(() -> processQueue(discreteQueue, "lntds_discrete_data"));
        }
        System.gc();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            stop();
            executor.shutdown();
            try {
                if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
            }
        }));
    }
    private void processQueue(LinkedBlockingQueue<String[]> queue, String tableName) {
        while (running) {
            try {
                LinkedList<String[]> batchParams = new LinkedList<>();
                for (int i = 0; i < BATCH_SIZE; i++) {
                    String[] item = queue.poll(7, TimeUnit.MILLISECONDS);
                    if (item != null) {
                        batchParams.add(item);
                    }
                }
                if (!batchParams.isEmpty()) {
                    insertDataIntoDatabase(batchParams, tableName);
//                    batchParams.clear();
                    batchParams=null;
                }
            } catch (Exception e) {
                LOGGER.error("Error processing queue: ", e);
            }
        }
    }


// -- working fine - 20% cpu
    private void insertDataIntoDatabase(List<String[]> batchParams, String tableName) {
        long startTime = System.currentTimeMillis();

        // SQL statement preparation based on table type
        final String analogSql = "INSERT INTO " + tableName +
                " (asset_id, transmit_timestamp, record_timestamp, input_measurement_name, measurement_name, measurement_input_value, measurement_analog_value, unit_of_measurement) VALUES ";
        final String discreteSql = "INSERT INTO " + tableName +
                " (asset_id, transmit_timestamp, record_timestamp, input_measurement_name, measurement_name, measurement_input_value, measurement_discrete_value) VALUES ";

        StringBuilder sqlBuilder = new StringBuilder(tableName.contains("analog") ? analogSql : discreteSql);
        String valueTemplate = tableName.contains("analog") ? "(?, ?, ?, ?, ?, ?, ?, ?)" : "(?, ?, ?, ?, ?, ?, ?)";

        // Constructing a bulk SQL insert statement
        for (int i = 0; i < batchParams.size(); i++) {
            sqlBuilder.append(valueTemplate);
            if (i < batchParams.size() - 1) {
                sqlBuilder.append(", ");
            }
        }
        String dbUrl = String.format("jdbc:mysql://%s:3306/%s", serverConfig.getMysqlIpAddress(), serverConfig.getMysqlDb());

        // Getting a connection from the pool
        try (Connection connection = DriverManager.getConnection(dbUrl, serverConfig.getMysqlUserName(), serverConfig.getMysqlPassword());
             PreparedStatement preparedStatement = connection.prepareStatement(sqlBuilder.toString())) {

            // Setting parameters for the batch
            int paramIndex = 1;
            for (String[] params : batchParams) {
                for (String param : params) {
                    preparedStatement.setString(paramIndex++, param);
                }
            }
            // Executing the batch
            preparedStatement.executeUpdate();
            LOGGER.info("Data inserted into table " + tableName +" from "+ serverConfig.getName() + " into "+
                    serverConfig.getMysqlDb()+" in " + (System.currentTimeMillis() - startTime) + " milliseconds at " + LocalDateTime.now());
            sqlBuilder=null;
        } catch (SQLException e) {
            LOGGER.error("Error inserting data into database: ", e);
        }
    }

    void stop() {
        running = false;
    }

    public static String getAssetId(String channelName) {
        return channelMap.get(channelName);
    }

    @Override
    public void run() {
        final ExecutorService threadPool = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
        LOGGER.info("inside run method at " + LocalDateTime.now());
        for (int i = 0; i < THREAD_POOL_SIZE; i++) {
            int start = i * TOPIC_PER_THREAD;
            int end = (i == THREAD_POOL_SIZE - 1) ? serverConfig.getRedisTopics().size() : start + TOPIC_PER_THREAD;
            threadPool.submit(() -> {
                Jedis jedis = serverConfig.getJedisPool().getResource();
                jedis.subscribe(new JedisPubSub() {
                    @Override
                    public void onMessage(String channel, String message) {
                        String[] parts = message.split(",");
                        if (!"epoch".equals(parts[parts.length-1].split(" ")[0])) {
                            LOGGER.error("Error: Improper data format.");
                            return;
                        }
                        String assetId = getAssetId(channel);
                        long milliseconds = TimeUnit.MILLISECONDS.convert(Long.parseLong(parts[parts.length - 1].split(" ")[1]), TimeUnit.NANOSECONDS);
                        Timestamp timeStamp = new Timestamp(milliseconds);
                      //  long timeStamp= Long.parseLong(parts[0].split(" ")[1]) * 1000;
                        LOGGER.info("redis time: "+timeStamp+ " at current time "+System.currentTimeMillis());
                        for (int i = 1; i < parts.length-1; i++) {
                            String[] measurement = parts[i].split(" ");
                            String meas = measurement[0];
                            double value = Double.parseDouble(measurement[1]);
                            String key = serverConfig.getMysqlDb() + assetId + meas;
//                            String key = new StringBuilder(serverConfig.getMysqlDb())
//                                    .append(assetId)
//                                    .append(meas)
//                                    .toString();
                            AssetMeasDTO assetMeasDTO = AssetMeasDBMaps.find(key);
                            key=null;
                            if (assetMeasDTO != null) {
                                String[] params;
                                if (assetMeasDTO.getMeasurementType().equalsIgnoreCase("Analog")) {
                                    params = new String[]{assetId, dateFormat.format(timeStamp), dateFormat.format(System.currentTimeMillis()), meas, meas, Double.toString(value), Double.toString(value), assetMeasDTO.getUom()};
                                    analogQueue.add(params);
                                } else {
                                    params = new String[]{assetId, dateFormat.format(timeStamp), dateFormat.format(System.currentTimeMillis()), meas, meas, Double.toString(value), Integer.toString((int) value)};
                                    discreteQueue.add(params);
                                }
                                params = null;
                                assetMeasDTO=null;
                            }else {
                                //LOGGER.error("Error in the measurement. No such measurement is found");
                            }
                            measurement = null;
                        }
                        parts=null;
                    }
                }, serverConfig.getRedisTopics().subList(start, end).toArray(new String[0]));
            });
            start();
        }
    }
}