/*
 * Copyright (c) 2018, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.carbon.apimgt.stat.migration.util;

import javax.sql.DataSource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.carbon.apimgt.stat.migration.APIMStatMigrationException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class DatabaseManager {

    private static volatile DataSource previous_dataSource = null;
    private static volatile DataSource new_dataSource = null;
    private static final String PREVIOUS_DATA_SOURCE_NAME = "jdbc/WSO2AM_STATS_DB"; // Take this config from the migration-config of this client
    private static final String NEW_DATA_SOURCE_NAME = "jdbc/APIM_ANALYTICS_DB";

    private static final Log log = LogFactory.getLog(DatabaseManager.class);

    public static void initialize() throws APIMStatMigrationException {
//        try {
//            Context ctx = new InitialContext();
//            previous_dataSource = (DataSource) ctx.lookup(PREVIOUS_DATA_SOURCE_NAME);
//        } catch (NamingException e) {
//            throw new APIMStatMigrationException(
//                    "Error while looking up the data source: " + PREVIOUS_DATA_SOURCE_NAME);
//        }
//        try {
//            Context ctx = new InitialContext();
//            new_dataSource = (DataSource) ctx.lookup(NEW_DATA_SOURCE_NAME);
//        } catch (NamingException e) {
//            throw new APIMStatMigrationException(
//                    "Error while looking up the data source: " + NEW_DATA_SOURCE_NAME);
//        }

    }

    public static void migrateDestinationSummaryTable() throws APIMStatMigrationException {
        Connection con1 = null;
        Connection con2 = null;
        PreparedStatement statement1 = null;
        PreparedStatement statement2 = null;
        ResultSet resultSetRetrieved = null;
        try {
            con1 = DriverManager.getConnection("jdbc:mysql://localhost:3306/tstatdb?autoReconnect=true", "root", "tharika@123");
            con2 = DriverManager.getConnection("jdbc:mysql://localhost:3306/geolocationstatdb?autoReconnect=true", "root", "tharika@123");
            String retrieveQuery = "SELECT * FROM " + APIMStatMigrationConstants.API_DESTINATION_SUMMARY;
            String insertQuery = "INSERT INTO " + APIMStatMigrationConstants.API_PER_DESTINATION_AGG
                    + "_DAYS(apiName, apiVersion, apiCreator, apiContext, destination, AGG_COUNT, apiHostname, "
                    + "AGG_TIMESTAMP, AGG_EVENT_TIMESTAMP) VALUES(?,?,?,?,?,?,?,?,?)";
            statement1 = con1.prepareStatement(retrieveQuery);
            statement2 = con2.prepareStatement(insertQuery);
            resultSetRetrieved = statement1.executeQuery();
            while (resultSetRetrieved.next()) {
                String api = resultSetRetrieved.getString("api");
                String version = resultSetRetrieved.getString("version");
                String apiPublisher = resultSetRetrieved.getString("apiPublisher");
                String context = resultSetRetrieved.getString("context");
                String destination = resultSetRetrieved.getString("destination");
                int total_request_count = resultSetRetrieved.getInt("total_request_count");
                String hostName = resultSetRetrieved.getString("hostName");
                int year = resultSetRetrieved.getInt("year");
                int month = resultSetRetrieved.getInt("month");
                int day = resultSetRetrieved.getInt("day");
                String time = resultSetRetrieved.getString("time");
                statement2.setString(1, api);
                statement2.setString(2, version);
                statement2.setString(3, apiPublisher);
                statement2.setString(4, context);
                statement2.setString(5, destination);
                statement2.setInt(6, total_request_count);
                statement2.setString(7, hostName);
                statement2.setInt(8, year);
                statement2.setString(9, time);
            }
        } catch (SQLException e) {
            String msg = "Error occurred while connecting to and querying from the database";
            log.error(msg, e);
            throw new APIMStatMigrationException(msg, e);
        } finally {
            closeDatabaseLinks(resultSetRetrieved, statement1, con1);
            closeDatabaseLinks(null, statement2, con2);
        }

    }

    /**
     * This method is used to close the ResultSet, PreparedStatement and Connection after getting data from the DB
     * This is called if a "PreparedStatement" is used to fetch results from the DB
     *
     * @param resultSet         ResultSet returned from the database query
     * @param preparedStatement prepared statement used in the database query
     * @param connection        DB connection used to get data from the database
     */
    private static void closeDatabaseLinks(ResultSet resultSet, PreparedStatement preparedStatement, Connection connection) {

        if (resultSet != null) {
            try {
                resultSet.close();
            } catch (SQLException e) {
                //this is logged and the process is continued because the query has executed
                log.error("Error occurred while closing the result set from JDBC database.", e);
            }
        }
        if (preparedStatement != null) {
            try {
                preparedStatement.close();
            } catch (SQLException e) {
                //this is logged and the process is continued because the query has executed
                log.error("Error occurred while closing the prepared statement from JDBC database.", e);
            }
        }
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                //this is logged and the process is continued because the query has executed
                log.error("Error occurred while closing the JDBC database connection.", e);
            }
        }
    }
}
