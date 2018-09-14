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

public class APIMStatMigrationConstants {

    //Table names of previous database
    public static final String API_DESTINATION_SUMMARY = "API_DESTINATION_SUMMARY";
    public static final String API_PER_DESTINATION_AGG = "ApiPerDestinationAgg";

    public static final String API_RESOURCE_USAGE_SUMMARY = "API_Resource_USAGE_SUMMARY";
    public static final String API_RESOURCE_PATH_AGG = "ApiResourcePathPerAppAgg";

    public static final String API_VERSION_USAGE_SUMMARY = "API_VERSION_USAGE_SUMMARY";
    public static final String API_VERSION_USAGE_AGG = "ApiVersionPerAppAgg";

    public static final String API_LAST_ACCESS_TIME_SUMMARY = "API_LAST_ACCESS_TIME_SUMMARY";
    public static final String API_LAST_ACCESS_SUMMARY_AGG = "ApiLastAccessSummary";

    public static final String API_FAULT_SUMMARY = "API_FAULT_SUMMARY";
    public static final String API_FAULTY_INVOCATION_AGG= "ApiFaultyInvocationAgg";

    public static final String API_REQ_USR_BROW_SUMMARY = "API_REQ_USER_BROW_SUMMARY";
    public static final String API_USER_BROWSER_AGG = "ApiUserBrowserAgg";

    public static final String API_REQ_GEO_LOC_SUMMARY = "API_REQ_GEO_LOC_SUMMARY";
    public static final String API_GEO_LOCATION_AGG = "GeoLocationAgg";

    public static final String API_EXE_TME_DAY_SUMMARY = "API_EXE_TME_DAY_SUMMARY";
    public static final String API_EXE_TIME_HOUR_SUMMARY = "API_EXE_TIME_HOUR_SUMMARY";
    public static final String API_EXE_TIME_MIN_SUMMARY = "API_EXE_TIME_MIN_SUMMARY";
    public static final String API_EXEC_TIME_AGG = "ApiExecutionTimeAgg";

    public static final String API_THROTTLED_OUT_SUMMARY = "API_THROTTLED_OUT_SUMMARY";
    public static final String API_THROTTLED_OUT_AGG = "ApiThrottledOutAgg";
    public static final String APIM_REQ_COUNT_AGG = "APIM_ReqCountAgg";

    //Osgi constants
    public static final String ARG_MIGRATE_STATS = "migrateStats";

    //Other constants
    public static final String TIMESTAMP_PATTERN = "yyyy-MM-dd HH:mm";
    public static final String TIMESTAMP_DAY_PATTERN = "yyyy-M-dd";
    public static final String TIMESTAMP_HOUR_PATTERN = "yyyy-M-dd HH";
    public static final String TIMESTAMP_MINUTE_PATTERN = "yyyy-M-dd HH:mm";
}
