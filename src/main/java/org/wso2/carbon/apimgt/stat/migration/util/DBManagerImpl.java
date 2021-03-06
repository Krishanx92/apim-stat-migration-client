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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.wso2.carbon.apimgt.stat.migration.APIMStatMigrationException;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class DBManagerImpl implements DBManager {

    private static final Log log = LogFactory.getLog(DBManagerImpl.class);

    private static volatile DataSource oldStatsDataSource = null;
    private static volatile DataSource newStatsDataSource = null;
    private static volatile DataSource apimDataSource = null;
    private static final String OLD_STATS_DATA_SOURCE_NAME = "jdbc/WSO2AM_STATS_DB";
    private static final String NEW_STATS_DATA_SOURCE_NAME = "jdbc/APIM_ANALYTICS_DB";
    private static final String APIM_DATA_SOURCE_NAME = "jdbc/WSO2AM_DB";

    /**
     * This method initializes the datasources required for the migration of the stats dbs
     *
     * @throws APIMStatMigrationException when there is an error looking up the datasources
     */
    @Override
    public void initialize() throws APIMStatMigrationException {
        try {
            Context ctx = new InitialContext();
            oldStatsDataSource = (DataSource) ctx.lookup(OLD_STATS_DATA_SOURCE_NAME);
        } catch (NamingException e) {
            String msg = "Error while looking up the data source: " + OLD_STATS_DATA_SOURCE_NAME;
            log.error(msg);
            throw new APIMStatMigrationException(msg);
        }

        try {
            Context ctx = new InitialContext();
            newStatsDataSource = (DataSource) ctx.lookup(NEW_STATS_DATA_SOURCE_NAME);
        } catch (NamingException e) {
            String msg = "Error while looking up the data source: " + NEW_STATS_DATA_SOURCE_NAME;
            log.error(msg);
            throw new APIMStatMigrationException(msg);
        }

        try {
            Context ctx = new InitialContext();
            apimDataSource = (DataSource) ctx.lookup(APIM_DATA_SOURCE_NAME);
        } catch (NamingException e) {
            String msg = "Error while looking up the data source: " + APIM_DATA_SOURCE_NAME;
            log.error(msg);
            throw new APIMStatMigrationException(msg);
        }
    }

    /**
     * This method migrates the data related to the API_DESTINATION_SUMMARY table
     *
     * @throws APIMStatMigrationException on error
     */
    @Override
    public void migrateDestinationSummaryTable() throws APIMStatMigrationException {
        Connection con1 = null;
        Connection con2 = null;
        PreparedStatement statement1 = null;
        PreparedStatement statement2 = null;
        ResultSet resultSetRetrieved = null;
        try {
            con1 = oldStatsDataSource.getConnection();
            con2 = newStatsDataSource.getConnection();
            String retrieveQuery = "SELECT * FROM " + APIMStatMigrationConstants.API_DESTINATION_SUMMARY;
            String insertQuery = "INSERT INTO " + APIMStatMigrationConstants.API_PER_DESTINATION_AGG
                    + "_DAYS(apiName, apiVersion, apiCreator, apiContext, destination, AGG_COUNT, apiHostname, "
                    + "AGG_TIMESTAMP, AGG_EVENT_TIMESTAMP, AGG_LAST_EVENT_TIMESTAMP, gatewayType, label, regionalID) "
                    + "VALUES(?,?,?,?,?,?,?,?,?,?,'SYNAPSE','Synapse','default')";
            statement1 = con1.prepareStatement(retrieveQuery);
            statement2 = con2.prepareStatement(insertQuery);
            resultSetRetrieved = statement1.executeQuery();
            while (resultSetRetrieved.next()) {
                String api = resultSetRetrieved.getString("api");
                String version = resultSetRetrieved.getString("version");
                String apiPublisher = resultSetRetrieved.getString("apiPublisher");
                String context = resultSetRetrieved.getString("context");
                String destination = resultSetRetrieved.getString("destination");
                long total_request_count = resultSetRetrieved.getLong("total_request_count");
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
                statement2.setLong(6, total_request_count);
                statement2.setString(7, hostName);
                String dayInString = year + "-" + month + "-" + day;
                statement2.setLong(8, getTimestampOfDay(dayInString));
                statement2.setLong(9, getTimestamp(time));
                statement2.setLong(10, getTimestamp(time)); //same as AGG_EVENT_TIMESTAMP
                statement2.executeUpdate();
            }
        } catch (SQLException e) {
            String msg = "Error occurred while connecting to and querying from the database";
            log.error(msg, e);
            throw new APIMStatMigrationException(msg, e);
        } catch (Exception e) {
            String msg = "Generic error occurred while connecting to the database";
            log.error(msg, e);
            throw new APIMStatMigrationException(msg, e);
        } finally {
            closeDatabaseLinks(resultSetRetrieved, statement1, con1);
            closeDatabaseLinks(null, statement2, con2);
        }
    }

    /**
     * This method migrates the data related to the API_RESOURCE_USAGE_SUMMARY table
     *
     * @throws APIMStatMigrationException on error
     */
    @Override
    public void migrateResourceUsageSummaryTable() throws APIMStatMigrationException {
        Connection con1 = null;
        Connection con2 = null;
        Connection con3 = null;
        PreparedStatement statement1 = null;
        PreparedStatement statement2 = null;
        PreparedStatement statement3 = null;
        ResultSet resultSetRetrieved = null;
        ResultSet resultSetFromAMDB = null;
        try {
            con1 = oldStatsDataSource.getConnection();
            con2 = newStatsDataSource.getConnection();
            con3 = apimDataSource.getConnection();
            String consumerKeyMappingQuery = "select APPLICATION_ID from AM_APPLICATION_KEY_MAPPING WHERE CONSUMER_KEY=?";
            String retrieveQuery = "SELECT * FROM " + APIMStatMigrationConstants.API_RESOURCE_USAGE_SUMMARY;
            String insertQuery = "INSERT INTO " + APIMStatMigrationConstants.API_RESOURCE_PATH_AGG
                    + "_DAYS(apiName, apiVersion, apiCreator, apiResourceTemplate, apiContext, apiMethod, AGG_COUNT, "
                    + "apiHostname, AGG_TIMESTAMP, AGG_EVENT_TIMESTAMP, AGG_LAST_EVENT_TIMESTAMP, applicationId, "
                    + "gatewayType, label, regionalID) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,'SYNAPSE','Synapse','default')";
            statement1 = con1.prepareStatement(retrieveQuery);
            statement2 = con2.prepareStatement(insertQuery);
            statement3 = con3.prepareStatement(consumerKeyMappingQuery);
            resultSetRetrieved = statement1.executeQuery();
            while (resultSetRetrieved.next()) {
                String api = resultSetRetrieved.getString("api");
                String version = resultSetRetrieved.getString("version");
                String apiPublisher = resultSetRetrieved.getString("apiPublisher");
                //--------------------------
                String consumerKey = resultSetRetrieved.getString("consumerKey");
                statement3.setString(1, consumerKey);
                resultSetFromAMDB = statement3.executeQuery();
                int applicationId = -1;
                while (resultSetFromAMDB.next()) {
                    applicationId = resultSetFromAMDB.getInt("APPLICATION_ID");
                }
                //-------------------------------
                String resourcePath = resultSetRetrieved.getString("resourcePath");
                String context = resultSetRetrieved.getString("context");
                String method = resultSetRetrieved.getString("method");
                long total_request_count = resultSetRetrieved.getLong("total_request_count");
                String hostName = resultSetRetrieved.getString("hostName");
                int year = resultSetRetrieved.getInt("year");
                int month = resultSetRetrieved.getInt("month");
                int day = resultSetRetrieved.getInt("day");
                String time = resultSetRetrieved.getString("time");
                statement2.setString(1, api);
                statement2.setString(2, version);
                statement2.setString(3, apiPublisher);
                statement2.setString(4, resourcePath);
                statement2.setString(5, context);
                statement2.setString(6, method);
                statement2.setLong(7, total_request_count);
                statement2.setString(8, hostName);
                String dayInString = year + "-" + month + "-" + day;
                statement2.setLong(9, getTimestampOfDay(dayInString));
                statement2.setLong(10, getTimestamp(time));
                statement2.setLong(11, getTimestamp(time));
                if (applicationId != -1) {
                    statement2.setString(12, Integer.toString(applicationId));
                } else {
                    String errorMsg = "Error occurred while retrieving applicationId for consumer key : " + consumerKey;
                    log.error(errorMsg);
                    throw new APIMStatMigrationException(errorMsg);
                }
                statement2.executeUpdate();
            }
        } catch (SQLException e) {
            String msg = "Error occurred while connecting to and querying from the database";
            log.error(msg, e);
            throw new APIMStatMigrationException(msg, e);
        } catch (Exception e) {
            String msg = "Generic error occurred while connecting to the database";
            log.error(msg, e);
            throw new APIMStatMigrationException(msg, e);
        } finally {
            closeDatabaseLinks(resultSetRetrieved, statement1, con1);
            closeDatabaseLinks(null, statement2, con2);
            closeDatabaseLinks(resultSetFromAMDB, statement3, con3);
        }
    }

    /**
     * This method migrates the data related to the API_VERSION_USAGE_SUMMARY table
     *
     * @throws APIMStatMigrationException on error
     */
    @Override
    public void migrateVersionUsageSummaryTable() throws APIMStatMigrationException {
        Connection con1 = null;
        Connection con2 = null;
        PreparedStatement statement1 = null;
        PreparedStatement statement2 = null;
        ResultSet resultSetRetrieved = null;
        try {
            con1 = oldStatsDataSource.getConnection();
            con2 = newStatsDataSource.getConnection();
            String retrieveQuery = "SELECT * FROM " + APIMStatMigrationConstants.API_VERSION_USAGE_SUMMARY;
            String insertQuery = "INSERT INTO " + APIMStatMigrationConstants.API_VERSION_USAGE_AGG
                    + "_DAYS(apiName, apiVersion, apiCreator, apiContext, AGG_COUNT, apiHostname, AGG_TIMESTAMP, "
                    + "AGG_EVENT_TIMESTAMP, AGG_LAST_EVENT_TIMESTAMP, applicationId, gatewayType, label, regionalID) "
                    + "VALUES(?,?,?,?,?,?,?,?,?,'','SYNAPSE','Synapse','default')";
            statement1 = con1.prepareStatement(retrieveQuery);
            statement2 = con2.prepareStatement(insertQuery);
            resultSetRetrieved = statement1.executeQuery();
            while (resultSetRetrieved.next()) {
                String api = resultSetRetrieved.getString("api");
                String version = resultSetRetrieved.getString("version");
                String apiPublisher = resultSetRetrieved.getString("apiPublisher");
                String context = resultSetRetrieved.getString("context");
                long total_request_count = resultSetRetrieved.getLong("total_request_count");
                String hostName = resultSetRetrieved.getString("hostName");
                int year = resultSetRetrieved.getInt("year");
                int month = resultSetRetrieved.getInt("month");
                int day = resultSetRetrieved.getInt("day");
                String time = resultSetRetrieved.getString("time");
                statement2.setString(1, api);
                statement2.setString(2, version);
                statement2.setString(3, apiPublisher);
                statement2.setString(4, context);
                statement2.setLong(5, total_request_count);
                statement2.setString(6, hostName);
                String dayInString = year + "-" + month + "-" + day;
                statement2.setLong(7, getTimestampOfDay(dayInString));
                statement2.setLong(8, getTimestamp(time));
                statement2.setLong(9, getTimestamp(time)); //same as AGG_EVENT_TIMESTAMP
                statement2.executeUpdate();
            }
        } catch (SQLException e) {
            String msg = "Error occurred while connecting to and querying from the database";
            log.error(msg, e);
            throw new APIMStatMigrationException(msg, e);
        } catch (Exception e) {
            String msg = "Generic error occurred while connecting to the database";
            log.error(msg, e);
            throw new APIMStatMigrationException(msg, e);
        } finally {
            closeDatabaseLinks(resultSetRetrieved, statement1, con1);
            closeDatabaseLinks(null, statement2, con2);
        }
    }

    /**
     * This method migrates the data related to the API_LAST_ACCESS_TIME_SUMMARY table
     *
     * @throws APIMStatMigrationException on error
     */
    @Override
    public void migrateLastAccessTimeSummaryTable() throws APIMStatMigrationException {
        Connection con1 = null;
        Connection con2 = null;
        PreparedStatement statement1 = null;
        PreparedStatement statement2 = null;
        ResultSet resultSetRetrieved = null;
        try {
            con1 = oldStatsDataSource.getConnection();
            con2 = newStatsDataSource.getConnection();
            String retrieveQuery = "SELECT * FROM " + APIMStatMigrationConstants.API_LAST_ACCESS_TIME_SUMMARY;
            String insertQuery = "INSERT INTO " + APIMStatMigrationConstants.API_LAST_ACCESS_SUMMARY_AGG
                    + "(apiCreatorTenantDomain, apiCreator, apiName, apiVersion, applicationOwner, apiContext, lastAccessTime) VALUES(?,?,?,?,?,?,?)";
            statement1 = con1.prepareStatement(retrieveQuery);
            statement2 = con2.prepareStatement(insertQuery);
            resultSetRetrieved = statement1.executeQuery();
            while (resultSetRetrieved.next()) {
                String tenantDomain = resultSetRetrieved.getString("tenantDomain");
                String apiPublisher = resultSetRetrieved.getString("apiPublisher");
                String api = resultSetRetrieved.getString("api");
                String version = resultSetRetrieved.getString("version");
                String userId = resultSetRetrieved.getString("userId");
                String context = resultSetRetrieved.getString("context");
                long max_request_time = resultSetRetrieved.getLong("max_request_time");
                statement2.setString(1, tenantDomain);
                statement2.setString(2, apiPublisher);
                statement2.setString(3, api);
                statement2.setString(4, version);
                statement2.setString(5, userId);
                statement2.setString(6, context);
                statement2.setLong(7, max_request_time);
                statement2.executeUpdate();
            }
        } catch (SQLException e) {
            String msg = "Error occurred while connecting to and querying from the database";
            log.error(msg, e);
            throw new APIMStatMigrationException(msg, e);
        } catch (Exception e) {
            String msg = "Generic error occurred while connecting to the database";
            log.error(msg, e);
            throw new APIMStatMigrationException(msg, e);
        } finally {
            closeDatabaseLinks(resultSetRetrieved, statement1, con1);
            closeDatabaseLinks(null, statement2, con2);
        }
    }

    /**
     * This method migrates the data related to the API_FAULT_SUMMARY table
     *
     * @throws APIMStatMigrationException on error
     */
    @Override
    public void migrateFaultSummaryTable() throws APIMStatMigrationException {
        Connection con1 = null;
        Connection con2 = null;
        Connection con3 = null;
        PreparedStatement statement1 = null;
        PreparedStatement statement2 = null;
        PreparedStatement statement3 = null;
        ResultSet resultSetRetrieved = null;
        ResultSet resultSetFromAMDB = null;
        try {
            con1 = oldStatsDataSource.getConnection();
            con2 = newStatsDataSource.getConnection();
            con3 = apimDataSource.getConnection();
            String consumerKeyMappingQuery = "SELECT APPLICATION_ID FROM AM_APPLICATION_KEY_MAPPING WHERE CONSUMER_KEY=?";
            String retrieveQuery = "SELECT * FROM " + APIMStatMigrationConstants.API_FAULT_SUMMARY;
            String insertQuery = "INSERT INTO " + APIMStatMigrationConstants.API_FAULTY_INVOCATION_AGG
                    + "_DAYS(apiName, apiVersion, apiCreator, applicationId, apiContext, AGG_COUNT, hostname, "
                    + "AGG_TIMESTAMP, AGG_EVENT_TIMESTAMP, AGG_LAST_EVENT_TIMESTAMP, regionalID) VALUES(?,?,?,?,?,?,?,?,?,?,'default')";
            statement1 = con1.prepareStatement(retrieveQuery);
            statement2 = con2.prepareStatement(insertQuery);
            statement3 = con3.prepareStatement(consumerKeyMappingQuery);
            resultSetRetrieved = statement1.executeQuery();
            while (resultSetRetrieved.next()) {
                String api = resultSetRetrieved.getString("api");
                String version = resultSetRetrieved.getString("version");
                String apiPublisher = resultSetRetrieved.getString("apiPublisher");
                //--------------------------
                String consumerKey = resultSetRetrieved.getString("consumerKey");
                statement3.setString(1, consumerKey);
                resultSetFromAMDB = statement3.executeQuery();
                int applicationId = -1;
                while (resultSetFromAMDB.next()) {
                    applicationId = resultSetFromAMDB.getInt("APPLICATION_ID");
                }
                //-------------------------------
                String context = resultSetRetrieved.getString("context");
                long total_fault_count = resultSetRetrieved.getLong("total_fault_count");
                String hostName = resultSetRetrieved.getString("hostName");
                int year = resultSetRetrieved.getInt("year");
                int month = resultSetRetrieved.getInt("month");
                int day = resultSetRetrieved.getInt("day");
                String time = resultSetRetrieved.getString("time");
                statement2.setString(1, api);
                statement2.setString(2, version);
                statement2.setString(3, apiPublisher);
                if (applicationId != -1) {
                    statement2.setString(4, Integer.toString(applicationId));
                } else {
                    String errorMsg = "Error occurred while retrieving applicationId for consumer key : " + consumerKey;
                    log.error(errorMsg);
                    throw new APIMStatMigrationException(errorMsg);
                }
                statement2.setString(5, context);
                statement2.setLong(6, total_fault_count);
                statement2.setString(7, hostName);
                String dayInString = year + "-" + month + "-" + day;
                statement2.setLong(8, getTimestampOfDay(dayInString));
                statement2.setLong(9, getTimestamp(time));
                statement2.setLong(10, getTimestamp(time));
                statement2.executeUpdate();
            }
        } catch (SQLException e) {
            String msg = "Error occurred while connecting to and querying from the database";
            log.error(msg, e);
            throw new APIMStatMigrationException(msg, e);
        } catch (Exception e) {
            String msg = "Generic error occurred while connecting to the database";
            log.error(msg, e);
            throw new APIMStatMigrationException(msg, e);
        } finally {
            closeDatabaseLinks(resultSetRetrieved, statement1, con1);
            closeDatabaseLinks(null, statement2, con2);
            closeDatabaseLinks(resultSetFromAMDB, statement3, con3);
        }
    }

    /**
     * This method migrates the data related to the API_REQ_USR_BROW_SUMMARY table
     *
     * @throws APIMStatMigrationException on error
     */
    @Override
    public void migrateUserBrowserSummaryTable() throws APIMStatMigrationException {
        Connection con1 = null;
        Connection con2 = null;
        PreparedStatement statement1 = null;
        PreparedStatement statement2 = null;
        ResultSet resultSetRetrieved = null;
        try {
            con1 = oldStatsDataSource.getConnection();
            con2 = newStatsDataSource.getConnection();
            String retrieveQuery = "SELECT * FROM " + APIMStatMigrationConstants.API_REQ_USR_BROW_SUMMARY;
            String insertQuery = "INSERT INTO " + APIMStatMigrationConstants.API_USER_BROWSER_AGG
                    + "_DAYS(apiName, apiVersion, apiCreator, apiCreatorTenantDomain, AGG_COUNT, AGG_TIMESTAMP, "
                    + "AGG_EVENT_TIMESTAMP, AGG_LAST_EVENT_TIMESTAMP, operatingSystem, browser, apiContext, gatewayType,"
                    + " label, regionalID) VALUES(?,?,?,?,?,?,?,?,?,?,'','SYNAPSE','Synapse','default')";
            statement1 = con1.prepareStatement(retrieveQuery);
            statement2 = con2.prepareStatement(insertQuery);
            resultSetRetrieved = statement1.executeQuery();
            while (resultSetRetrieved.next()) {
                String api = resultSetRetrieved.getString("api");
                String version = resultSetRetrieved.getString("version");
                String apiPublisher = resultSetRetrieved.getString("apiPublisher");
                String tenantDomain = resultSetRetrieved.getString("tenantDomain");
                long total_request_count = resultSetRetrieved.getLong("total_request_count");
                int year = resultSetRetrieved.getInt("year");
                int month = resultSetRetrieved.getInt("month");
                int day = resultSetRetrieved.getInt("day");
                long time = resultSetRetrieved.getLong("requestTime");
                String os = resultSetRetrieved.getString("os");
                String browser = resultSetRetrieved.getString("browser");
                statement2.setString(1, api);
                statement2.setString(2, version);
                statement2.setString(3, apiPublisher);
                statement2.setString(4, tenantDomain);
                statement2.setLong(5, total_request_count);
                String dayInString = year + "-" + month + "-" + day;
                statement2.setLong(6, getTimestampOfDay(dayInString));
                statement2.setLong(7, time);
                statement2.setLong(8, time);
                statement2.setString(9, os);
                statement2.setString(10, browser);
                statement2.executeUpdate();
            }
        } catch (SQLException e) {
            String msg = "Error occurred while connecting to and querying from the database";
            log.error(msg, e);
            throw new APIMStatMigrationException(msg, e);
        } catch (Exception e) {
            String msg = "Generic error occurred while connecting to the database";
            log.error(msg, e);
            throw new APIMStatMigrationException(msg, e);
        } finally {
            closeDatabaseLinks(resultSetRetrieved, statement1, con1);
            closeDatabaseLinks(null, statement2, con2);
        }
    }

    /**
     * This method migrates the data related to the API_REQ_GEO_LOC_SUMMARY table
     *
     * @throws APIMStatMigrationException on error
     */
    @Override
    public void migrateGeoLocationSummaryTable() throws APIMStatMigrationException {
        Connection con1 = null;
        Connection con2 = null;
        PreparedStatement statement1 = null;
        PreparedStatement statement2 = null;
        ResultSet resultSetRetrieved = null;
        try {
            con1 = oldStatsDataSource.getConnection();
            con2 = newStatsDataSource.getConnection();
            String retrieveQuery = "SELECT * FROM " + APIMStatMigrationConstants.API_REQ_GEO_LOC_SUMMARY;
            String insertQuery = "INSERT INTO " + APIMStatMigrationConstants.API_GEO_LOCATION_AGG
                    + "_DAYS(apiName, apiVersion, apiCreator, apiCreatorTenantDomain, totalCount, AGG_TIMESTAMP, "
                    + "AGG_EVENT_TIMESTAMP, AGG_LAST_EVENT_TIMESTAMP, country, city, apiContext, regionalID) VALUES(?,?,?,?,?,?,?,?,?,?,'','default')";
            statement1 = con1.prepareStatement(retrieveQuery);
            statement2 = con2.prepareStatement(insertQuery);
            resultSetRetrieved = statement1.executeQuery();
            while (resultSetRetrieved.next()) {
                String api = resultSetRetrieved.getString("api");
                String version = resultSetRetrieved.getString("version");
                String apiPublisher = resultSetRetrieved.getString("apiPublisher");
                String tenantDomain = resultSetRetrieved.getString("tenantDomain");
                long total_request_count = resultSetRetrieved.getLong("total_request_count");
                int year = resultSetRetrieved.getInt("year");
                int month = resultSetRetrieved.getInt("month");
                int day = resultSetRetrieved.getInt("day");
                long time = resultSetRetrieved.getLong("requestTime");
                String country = resultSetRetrieved.getString("country");
                String city = resultSetRetrieved.getString("city");
                statement2.setString(1, api);
                statement2.setString(2, version);
                statement2.setString(3, apiPublisher);
                statement2.setString(4, tenantDomain);
                statement2.setLong(5, total_request_count);
                String dayInString = year + "-" + month + "-" + day;
                statement2.setLong(6, getTimestampOfDay(dayInString));
                statement2.setLong(7, time);
                statement2.setLong(8, time);
                statement2.setString(9, country);
                statement2.setString(10, city); //check if ok to be null
                statement2.executeUpdate();
            }
        } catch (SQLException e) {
            String msg = "Error occurred while connecting to and querying from the database";
            log.error(msg, e);
            throw new APIMStatMigrationException(msg, e);
        } catch (Exception e) {
            String msg = "Generic error occurred while connecting to the database";
            log.error(msg, e);
            throw new APIMStatMigrationException(msg, e);
        } finally {
            closeDatabaseLinks(resultSetRetrieved, statement1, con1);
            closeDatabaseLinks(null, statement2, con2);
        }
    }

    /**
     * This method migrates the data related to the API_EXE_TME_DAY_SUMMARY table
     *
     * @throws APIMStatMigrationException on error
     */
    @Override
    public void migrateExecutionTimeDaySummaryTable() throws APIMStatMigrationException {
        Connection con1 = null;
        Connection con2 = null;
        PreparedStatement statement1 = null;
        PreparedStatement statement2 = null;
        ResultSet resultSetRetrieved = null;
        try {
            con1 = oldStatsDataSource.getConnection();
            con2 = newStatsDataSource.getConnection();
            String retrieveQuery = "SELECT * FROM " + APIMStatMigrationConstants.API_EXE_TME_DAY_SUMMARY;
            String insertQuery = "INSERT INTO " + APIMStatMigrationConstants.API_EXEC_TIME_AGG
                    + "_DAYS(apiName, apiVersion, apiCreatorTenantDomain, apiCreator, AGG_SUM_responseTime, apiContext, "
                    + "AGG_SUM_securityLatency, AGG_SUM_throttlingLatency, AGG_SUM_requestMedLat, "
                    + "AGG_SUM_responseMedLat, AGG_SUM_backendLatency, AGG_SUM_otherLatency, AGG_TIMESTAMP, "
                    + "AGG_EVENT_TIMESTAMP, AGG_LAST_EVENT_TIMESTAMP, apiHostname, apiResourceTemplate, apiMethod, "
                    + "regionalID) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'','','','default')";
            statement1 = con1.prepareStatement(retrieveQuery);
            statement2 = con2.prepareStatement(insertQuery);
            resultSetRetrieved = statement1.executeQuery();
            while (resultSetRetrieved.next()) {
                String api = resultSetRetrieved.getString("api");
                String version = resultSetRetrieved.getString("version");
                String tenantDomain = resultSetRetrieved.getString("tenantDomain");
                String apiPublisher = resultSetRetrieved.getString("apiPublisher");
                long apiResponseTime = resultSetRetrieved.getLong("apiResponseTime");
                String context = resultSetRetrieved.getString("context");
                long securityLatency = resultSetRetrieved.getLong("securityLatency");
                long throttlingLatency = resultSetRetrieved.getLong("throttlingLatency");
                long requestMediationLatency = resultSetRetrieved.getLong("requestMediationLatency");
                long responseMediationLatency = resultSetRetrieved.getLong("responseMediationLatency");
                long backendLatency = resultSetRetrieved.getLong("backendLatency");
                long otherLatency = resultSetRetrieved.getLong("otherLatency");
                int year = resultSetRetrieved.getInt("year");
                int month = resultSetRetrieved.getInt("month");
                int day = resultSetRetrieved.getInt("day");
                long time = resultSetRetrieved.getLong("time");
                statement2.setString(1, api);
                statement2.setString(2, version);
                statement2.setString(3, tenantDomain);
                statement2.setString(4, apiPublisher);
                statement2.setLong(5, apiResponseTime);
                statement2.setString(6, context);
                statement2.setLong(7, securityLatency);
                statement2.setLong(8, throttlingLatency);
                statement2.setLong(9, requestMediationLatency);
                statement2.setLong(10, responseMediationLatency);
                statement2.setLong(11, backendLatency);
                statement2.setLong(12, otherLatency);
                String dayInString = year + "-" + month + "-" + day;
                statement2.setLong(13, getTimestampOfDay(dayInString));
                statement2.setLong(14, time);
                statement2.setLong(15, time);
                statement2.executeUpdate();
            }
        } catch (SQLException e) {
            String msg = "Error occurred while connecting to and querying from the database";
            log.error(msg, e);
            throw new APIMStatMigrationException(msg, e);
        } catch (Exception e) {
            String msg = "Generic error occurred while connecting to the database";
            log.error(msg, e);
            throw new APIMStatMigrationException(msg, e);
        } finally {
            closeDatabaseLinks(resultSetRetrieved, statement1, con1);
            closeDatabaseLinks(null, statement2, con2);
        }
    }

    /**
     * This method migrates the data related to the API_EXE_TIME_HOUR_SUMMARY table
     *
     * @throws APIMStatMigrationException on error
     */
    @Override
    public void migrateExecutionTimeHourSummaryTable() throws APIMStatMigrationException {
        Connection con1 = null;
        Connection con2 = null;
        PreparedStatement statement1 = null;
        PreparedStatement statement2 = null;
        ResultSet resultSetRetrieved = null;
        try {
            con1 = oldStatsDataSource.getConnection();
            con2 = newStatsDataSource.getConnection();
            String retrieveQuery = "SELECT * FROM " + APIMStatMigrationConstants.API_EXE_TIME_HOUR_SUMMARY;
            String insertQuery = "INSERT INTO " + APIMStatMigrationConstants.API_EXEC_TIME_AGG
                    + "_HOURS(apiName, apiVersion, apiCreatorTenantDomain, apiCreator, AGG_SUM_responseTime, apiContext, "
                    + "AGG_SUM_securityLatency, AGG_SUM_throttlingLatency, AGG_SUM_requestMedLat, "
                    + "AGG_SUM_responseMedLat, AGG_SUM_backendLatency, AGG_SUM_otherLatency, AGG_TIMESTAMP, "
                    + "AGG_EVENT_TIMESTAMP, AGG_LAST_EVENT_TIMESTAMP, apiHostname, apiResourceTemplate, apiMethod, "
                    + "regionalID) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'','','','default')";
            statement1 = con1.prepareStatement(retrieveQuery);
            statement2 = con2.prepareStatement(insertQuery);
            resultSetRetrieved = statement1.executeQuery();
            while (resultSetRetrieved.next()) {
                String api = resultSetRetrieved.getString("api");
                String version = resultSetRetrieved.getString("version");
                String tenantDomain = resultSetRetrieved.getString("tenantDomain");
                String apiPublisher = resultSetRetrieved.getString("apiPublisher");
                long apiResponseTime = resultSetRetrieved.getLong("apiResponseTime");
                String context = resultSetRetrieved.getString("context");
                long securityLatency = resultSetRetrieved.getLong("securityLatency");
                long throttlingLatency = resultSetRetrieved.getLong("throttlingLatency");
                long requestMediationLatency = resultSetRetrieved.getLong("requestMediationLatency");
                long responseMediationLatency = resultSetRetrieved.getLong("responseMediationLatency");
                long backendLatency = resultSetRetrieved.getLong("backendLatency");
                long otherLatency = resultSetRetrieved.getLong("otherLatency");
                int year = resultSetRetrieved.getInt("year");
                int month = resultSetRetrieved.getInt("month");
                int day = resultSetRetrieved.getInt("day");
                int hour = resultSetRetrieved.getInt("hour");
                long time = resultSetRetrieved.getLong("time");
                statement2.setString(1, api);
                statement2.setString(2, version);
                statement2.setString(3, tenantDomain);
                statement2.setString(4, apiPublisher);
                statement2.setLong(5, apiResponseTime);
                statement2.setString(6, context);
                statement2.setLong(7, securityLatency);
                statement2.setLong(8, throttlingLatency);
                statement2.setLong(9, requestMediationLatency);
                statement2.setLong(10, responseMediationLatency);
                statement2.setLong(11, backendLatency);
                statement2.setLong(12, otherLatency);
                String hourInString = year + "-" + month + "-" + day + " " + hour;
                statement2.setLong(13, getTimestampOfHour(hourInString));
                statement2.setLong(14, time);
                statement2.setLong(15, time);
                statement2.executeUpdate();
            }
        } catch (SQLException e) {
            String msg = "Error occurred while connecting to and querying from the database";
            log.error(msg, e);
            throw new APIMStatMigrationException(msg, e);
        } catch (Exception e) {
            String msg = "Generic error occurred while connecting to the database";
            log.error(msg, e);
            throw new APIMStatMigrationException(msg, e);
        } finally {
            closeDatabaseLinks(resultSetRetrieved, statement1, con1);
            closeDatabaseLinks(null, statement2, con2);
        }
    }

    /**
     * This method migrates the data related to the API_EXE_TIME_MIN_SUMMARY table
     *
     * @throws APIMStatMigrationException on error
     */
    @Override
    public void migrateExecutionTimeMinuteSummaryTable() throws APIMStatMigrationException {
        Connection con1 = null;
        Connection con2 = null;
        PreparedStatement statement1 = null;
        PreparedStatement statement2 = null;
        ResultSet resultSetRetrieved = null;
        try {
            con1 = oldStatsDataSource.getConnection();
            con2 = newStatsDataSource.getConnection();
            String retrieveQuery = "SELECT * FROM " + APIMStatMigrationConstants.API_EXE_TIME_MIN_SUMMARY;
            String insertQuery = "INSERT INTO " + APIMStatMigrationConstants.API_EXEC_TIME_AGG
                    + "_MINUTES(apiName, apiVersion, apiCreatorTenantDomain, apiCreator, AGG_SUM_responseTime, apiContext, "
                    + "AGG_SUM_securityLatency, AGG_SUM_throttlingLatency, AGG_SUM_requestMedLat, "
                    + "AGG_SUM_responseMedLat, AGG_SUM_backendLatency, AGG_SUM_otherLatency, AGG_TIMESTAMP, "
                    + "AGG_EVENT_TIMESTAMP, AGG_LAST_EVENT_TIMESTAMP, apiHostname, apiResourceTemplate, apiMethod, "
                    + "regionalID) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'','','','default')";
            statement1 = con1.prepareStatement(retrieveQuery);
            statement2 = con2.prepareStatement(insertQuery);
            resultSetRetrieved = statement1.executeQuery();
            while (resultSetRetrieved.next()) {
                String api = resultSetRetrieved.getString("api");
                String version = resultSetRetrieved.getString("version");
                String tenantDomain = resultSetRetrieved.getString("tenantDomain");
                String apiPublisher = resultSetRetrieved.getString("apiPublisher");
                long apiResponseTime = resultSetRetrieved.getLong("apiResponseTime");
                String context = resultSetRetrieved.getString("context");
                long securityLatency = resultSetRetrieved.getLong("securityLatency");
                long throttlingLatency = resultSetRetrieved.getLong("throttlingLatency");
                long requestMediationLatency = resultSetRetrieved.getLong("requestMediationLatency");
                long responseMediationLatency = resultSetRetrieved.getLong("responseMediationLatency");
                long backendLatency = resultSetRetrieved.getLong("backendLatency");
                long otherLatency = resultSetRetrieved.getLong("otherLatency");
                int year = resultSetRetrieved.getInt("year");
                int month = resultSetRetrieved.getInt("month");
                int day = resultSetRetrieved.getInt("day");
                int hour = resultSetRetrieved.getInt("hour");
                int minute = resultSetRetrieved.getInt("minutes");
                long time = resultSetRetrieved.getLong("time");
                statement2.setString(1, api);
                statement2.setString(2, version);
                statement2.setString(3, tenantDomain);
                statement2.setString(4, apiPublisher);
                statement2.setLong(5, apiResponseTime);
                statement2.setString(6, context);
                statement2.setLong(7, securityLatency);
                statement2.setLong(8, throttlingLatency);
                statement2.setLong(9, requestMediationLatency);
                statement2.setLong(10, responseMediationLatency);
                statement2.setLong(11, backendLatency);
                statement2.setLong(12, otherLatency);
                String minuteInString = year + "-" + month + "-" + day + " " + hour + ":" + minute;
                statement2.setLong(13, getTimestampOfMinute(minuteInString));
                statement2.setLong(14, time);
                statement2.setLong(15, time);
                statement2.executeUpdate();
            }
        } catch (SQLException e) {
            String msg = "Error occurred while connecting to and querying from the database";
            log.error(msg, e);
            throw new APIMStatMigrationException(msg, e);
        } catch (Exception e) {
            String msg = "Generic error occurred while connecting to the database";
            log.error(msg, e);
            throw new APIMStatMigrationException(msg, e);
        } finally {
            closeDatabaseLinks(resultSetRetrieved, statement1, con1);
            closeDatabaseLinks(null, statement2, con2);
        }
    }

    /**
     * This method migrates the data related to the API_THROTTLED_OUT_SUMMARY table
     *
     * @throws APIMStatMigrationException on error
     */
    @Override
    public void migrateThrottledOutSummaryTable() throws APIMStatMigrationException {
        Connection con1 = null;
        Connection con2 = null;
        PreparedStatement statement1 = null;
        PreparedStatement statement2 = null;
        ResultSet resultSetRetrieved = null;
        try {
            con1 = oldStatsDataSource.getConnection();
            con2 = newStatsDataSource.getConnection();
            String retrieveQuery = "SELECT * FROM " + APIMStatMigrationConstants.API_THROTTLED_OUT_SUMMARY;
            String insertQuery = "INSERT INTO " + APIMStatMigrationConstants.API_THROTTLED_OUT_AGG
                    + "_DAYS(apiName, apiVersion, apiContext, apiCreator, applicationName, apiCreatorTenantDomain, AGG_TIMESTAMP, "
                    + "AGG_EVENT_TIMESTAMP, AGG_LAST_EVENT_TIMESTAMP, AGG_COUNT, throttledOutReason, applicationId, hostname, "
                    + "gatewayType) VALUES(?,?,?,?,?,?,?,?,?,?,?,'','','SYNAPSE')";
            statement1 = con1.prepareStatement(retrieveQuery);
            statement2 = con2.prepareStatement(insertQuery);
            resultSetRetrieved = statement1.executeQuery();
            while (resultSetRetrieved.next()) {
                String api = resultSetRetrieved.getString("api");
                String api_version = resultSetRetrieved.getString("api_version");
                String context = resultSetRetrieved.getString("context");
                String apiPublisher = resultSetRetrieved.getString("apiPublisher");
                String applicationName = resultSetRetrieved.getString("applicationName");
                String tenantDomain = resultSetRetrieved.getString("tenantDomain");
                long throttleout_count = resultSetRetrieved.getLong("throttleout_count");
                String throttledOutReason = resultSetRetrieved.getString("throttledOutReason");
                int year = resultSetRetrieved.getInt("year");
                int month = resultSetRetrieved.getInt("month");
                int day = resultSetRetrieved.getInt("day");
                String time = resultSetRetrieved.getString("time");
                statement2.setString(1, api);
                String version = api_version.split(":v")[1];
                statement2.setString(2, version);
                statement2.setString(3, context);
                statement2.setString(4, apiPublisher);
                statement2.setString(5, applicationName);
                statement2.setString(6, tenantDomain);
                String dayInString = year + "-" + month + "-" + day;
                statement2.setLong(7, getTimestampOfDay(dayInString));
                statement2.setLong(8, getTimestamp(time));
                statement2.setLong(9, getTimestamp(time));
                statement2.setLong(10, throttleout_count);
                statement2.setString(11, throttledOutReason);
                statement2.executeUpdate();
            }
        } catch (SQLException e) {
            String msg = "Error occurred while connecting to and querying from the database";
            log.error(msg, e);
            throw new APIMStatMigrationException(msg, e);
        } catch (Exception e) {
            String msg = "Generic error occurred while connecting to the database";
            log.error(msg, e);
            throw new APIMStatMigrationException(msg, e);
        } finally {
            closeDatabaseLinks(resultSetRetrieved, statement1, con1);
            closeDatabaseLinks(null, statement2, con2);
        }
    }

    /**
     * This method migrates the data related to the API_THROTTLED_OUT_SUMMARY table for success counts
     *
     * @throws APIMStatMigrationException on error
     */
    @Override
    public void migrateThrottledOutRequestCountSummaryTable() throws APIMStatMigrationException {
        Connection con1 = null;
        Connection con2 = null;
        PreparedStatement statement1 = null;
        PreparedStatement statement2 = null;
        ResultSet resultSetRetrieved = null;
        try {
            con1 = oldStatsDataSource.getConnection();
            con2 = newStatsDataSource.getConnection();
            String retrieveQuery = "SELECT api, api_version, apiPublisher, applicationName, tenantDomain, "
                    + "sum(throttleout_count) as throttledCount, sum(success_request_count) as successCount, year, month, day, time FROM " +
                    APIMStatMigrationConstants.API_THROTTLED_OUT_SUMMARY + " group by api, api_version, apiPublisher, "
                    + "tenantDomain, applicationName, year, month, day, week, time";
            String insertQuery = "INSERT INTO " + APIMStatMigrationConstants.APIM_REQ_COUNT_AGG
                    + "_DAYS(apiName, apiVersion, apiCreator, applicationName, apiCreatorTenantDomain, AGG_TIMESTAMP, "
                    + "AGG_EVENT_TIMESTAMP, AGG_SUM_successCount, AGG_SUM_throttleCount) VALUES(?,?,?,?,?,?,?,?,?)";
            statement1 = con1.prepareStatement(retrieveQuery);
            statement2 = con2.prepareStatement(insertQuery);
            resultSetRetrieved = statement1.executeQuery();
            while (resultSetRetrieved.next()) {
                String api = resultSetRetrieved.getString("api");
                String api_version = resultSetRetrieved.getString("api_version");
                String apiPublisher = resultSetRetrieved.getString("apiPublisher");
                String applicationName = resultSetRetrieved.getString("applicationName");
                String tenantDomain = resultSetRetrieved.getString("tenantDomain");
                long throttledCount = resultSetRetrieved.getLong("throttledCount");
                long successCount = resultSetRetrieved.getLong("successCount");
                int year = resultSetRetrieved.getInt("year");
                int month = resultSetRetrieved.getInt("month");
                int day = resultSetRetrieved.getInt("day");
                String time = resultSetRetrieved.getString("time");
                statement2.setString(1, api);
                String version = api_version.split(":v")[1];
                statement2.setString(2, version);
                statement2.setString(3, apiPublisher);
                statement2.setString(4, applicationName);
                statement2.setString(5, tenantDomain);
                String dayInString = year + "-" + month + "-" + day;
                statement2.setLong(6, getTimestampOfDay(dayInString));
                statement2.setLong(7, getTimestamp(time));
                statement2.setLong(8, successCount);
                statement2.setLong(9, throttledCount);
                statement2.executeUpdate();
            }
        } catch (SQLException e) {
            String msg = "Error occurred while connecting to and querying from the database";
            log.error(msg, e);
            throw new APIMStatMigrationException(msg, e);
        } catch (Exception e) {
            String msg = "Generic error occurred while connecting to the database";
            log.error(msg, e);
            throw new APIMStatMigrationException(msg, e);
        } finally {
            closeDatabaseLinks(resultSetRetrieved, statement1, con1);
            closeDatabaseLinks(null, statement2, con2);
        }
    }

    /**
     * This method migrates the data related to the API_REQUEST_SUMMARY table
     *
     * @throws APIMStatMigrationException on error
     */
    @Override
    public void migrateRequestSummaryTable() throws APIMStatMigrationException {
        Connection con1 = null;
        Connection con2 = null;
        Connection con3 = null;
        PreparedStatement statement1 = null;
        PreparedStatement statement2 = null;
        PreparedStatement statement3 = null;
        ResultSet resultSetRetrieved = null;
        ResultSet resultSetFromAMDB = null;
        try {
            con1 = oldStatsDataSource.getConnection();
            con2 = newStatsDataSource.getConnection();
            con3 = apimDataSource.getConnection();
            String consumerKeyMappingQuery = "SELECT APPLICATION_ID FROM AM_APPLICATION_KEY_MAPPING WHERE CONSUMER_KEY=?";
            String retrieveQuery = "SELECT * FROM " + APIMStatMigrationConstants.API_REQUEST_SUMMARY;
            String insertQuery = "INSERT INTO " + APIMStatMigrationConstants.API_USER_PER_APP_AGG
                    + "_DAYS(apiName, apiVersion, apiCreator, username, apiContext, AGG_COUNT, apiHostname, "
                    + "AGG_TIMESTAMP, AGG_EVENT_TIMESTAMP, AGG_LAST_EVENT_TIMESTAMP, applicationId, userTenantDomain, "
                    + "gatewayType, label, regionalID) VALUES(?,?,?,?,?,?,?,?,?,?,?,'','SYNAPSE','Synapse','default')";
            statement1 = con1.prepareStatement(retrieveQuery);
            statement2 = con2.prepareStatement(insertQuery);
            statement3 = con3.prepareStatement(consumerKeyMappingQuery);
            resultSetRetrieved = statement1.executeQuery();
            while (resultSetRetrieved.next()) {
                String api = resultSetRetrieved.getString("api");
                String version = resultSetRetrieved.getString("version");
                String apiPublisher = resultSetRetrieved.getString("apiPublisher");
                //--------------------------
                String consumerKey = resultSetRetrieved.getString("consumerKey");
                statement3.setString(1, consumerKey);
                resultSetFromAMDB = statement3.executeQuery();
                int applicationId = -1;
                while (resultSetFromAMDB.next()) {
                    applicationId = resultSetFromAMDB.getInt("APPLICATION_ID");
                }
                //-------------------------------
                String userId = resultSetRetrieved.getString("userId");
                String context = resultSetRetrieved.getString("context");
                long total_request_count = resultSetRetrieved.getLong("total_request_count");
                String hostName = resultSetRetrieved.getString("hostName");
                int year = resultSetRetrieved.getInt("year");
                int month = resultSetRetrieved.getInt("month");
                int day = resultSetRetrieved.getInt("day");
                String time = resultSetRetrieved.getString("time");
                statement2.setString(1, api);
                statement2.setString(2, version);
                statement2.setString(3, apiPublisher);
                statement2.setString(4, userId);
                statement2.setString(5, context);
                statement2.setLong(6, total_request_count);
                statement2.setString(7, hostName);
                String dayInString = year + "-" + month + "-" + day;
                statement2.setLong(8, getTimestampOfDay(dayInString));
                statement2.setLong(9, getTimestamp(time));
                statement2.setLong(10, getTimestamp(time));
                if (applicationId != -1) {
                    statement2.setString(11, Integer.toString(applicationId));
                } else {
                    String errorMsg = "Error occurred while retrieving applicationId for consumer key : " + consumerKey;
                    log.error(errorMsg);
                    throw new APIMStatMigrationException(errorMsg);
                }
                statement2.executeUpdate();
            }
        } catch (SQLException e) {
            String msg = "Error occurred while connecting to and querying from the database";
            log.error(msg, e);
            throw new APIMStatMigrationException(msg, e);
        } catch (Exception e) {
            String msg = "Generic error occurred while connecting to the database";
            log.error(msg, e);
            throw new APIMStatMigrationException(msg, e);
        } finally {
            closeDatabaseLinks(resultSetRetrieved, statement1, con1);
            closeDatabaseLinks(null, statement2, con2);
            closeDatabaseLinks(resultSetFromAMDB, statement3, con3);
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
    private static void closeDatabaseLinks(ResultSet resultSet, PreparedStatement preparedStatement,
            Connection connection) {

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

    /**
     * This method returns the date of form yyyy-MM-dd HH:mm as a timestamp
     *
     * @param date date as a string
     * @return the date in milliseconds
     */
    private static long getTimestamp(String date) {
        DateTimeFormatter formatter = DateTimeFormat.forPattern(APIMStatMigrationConstants.TIMESTAMP_PATTERN);
        DateTime dateTime = formatter.parseDateTime(date);
        return dateTime.getMillis();
    }

    /**
     * This method returns the date of form yyyy-M-dd as a timestamp
     *
     * @param date date as a string
     * @return the date in milliseconds
     */
    private static long getTimestampOfDay(String date) {
        DateTimeFormatter formatter = DateTimeFormat.forPattern(APIMStatMigrationConstants.TIMESTAMP_DAY_PATTERN);
        DateTime dateTime = formatter.parseDateTime(date);
        return dateTime.getMillis();
    }

    /**
     * This method returns the date of form yyyy-M-dd HH as a timestamp
     *
     * @param date date as a string
     * @return the date in milliseconds
     */
    private static long getTimestampOfHour(String date) {
        DateTimeFormatter formatter = DateTimeFormat.forPattern(APIMStatMigrationConstants.TIMESTAMP_HOUR_PATTERN);
        DateTime dateTime = formatter.parseDateTime(date);
        return dateTime.getMillis();
    }

    /**
     * This method returns the date of form yyyy-MM-dd HH:mm as a timestamp
     *
     * @param date date as a string
     * @return the date in milliseconds
     */
    private static long getTimestampOfMinute(String date) {
        DateTimeFormatter formatter = DateTimeFormat.forPattern(APIMStatMigrationConstants.TIMESTAMP_MINUTE_PATTERN);
        DateTime dateTime = formatter.parseDateTime(date);
        return dateTime.getMillis();
    }
}
