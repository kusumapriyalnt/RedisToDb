package com.lnt.ptd.redisToDB;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class AssetMeasDBMaps {
    static Logger logger = LogManager.getLogger(AssetMeasDBMaps.class);

    // map to store Measurement details of assets in the particular database
    public static Map<String, AssetMeasDTO> assetMeasDB = new ConcurrentHashMap<>();

    // method to find the measurement details of particular key
    public static AssetMeasDTO find(String key) {
        // returning the result DTO
        return assetMeasDB.get(key);
    }

    // method to update the map values
    public static void update(String dbName,String mysqlIpAddress,String mysqlUserName,String mysqlPassword) {
        // sql query to get the measurement details
        String sql = "select a.asset_id , c.measurement_name, c.measurement_type,c.uom from " +
                dbName + ".lntds_assets a, " + dbName + ".lntds_asset_types b ," + dbName +
                ".lntds_asset_types_measurements c where a.asset_type_seq = b.asset_type_seq and c.asset_type_seq = a.asset_type_seq";
        // constructing the url for the database connection
        String dbUrl = String.format("jdbc:mysql://%s:3306/%s", mysqlIpAddress, dbName);
        // trying to get the connection to db
        try(Connection conn = DriverManager.getConnection(dbUrl,mysqlUserName,mysqlPassword)){
            Statement stmt = conn.createStatement();
            // on executing the query, the result set is obtained
            ResultSet rs = stmt.executeQuery(sql);
            // iterating through the result set
            while(rs.next()){
                // initialising and assigning the values to the AssetMeasDTO object
                AssetMeasDTO dto = new AssetMeasDTO(
                        rs.getString("asset_id"),
                        rs.getString("measurement_name"),
                        rs.getString("measurement_type"),
                        rs.getString("uom")
                );
                String key = new StringBuilder(dbName)
                        .append(rs.getString("asset_id"))
                        .append(rs.getString("measurement_name"))
                        .toString();
                // adding the object to the map with particular key
                assetMeasDB.put(key, dto);
                key=null;
                dto=null;
            }
            logger.info("Total keys added for " + dbName + " is " + assetMeasDB.size());
        }catch(SQLException e){
            logger.error("Error while connecting to db or getting the AssetMeasDBMap", e);
        }
    }
}
