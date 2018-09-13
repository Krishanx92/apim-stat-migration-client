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

    //Osgi constants
    public static final String ARG_MIGRATE_STATS = "migrateStats";

    //Other constants
    public static final String TIMESTAMP_PATTERN = "yyyy-MM-dd HH:mm";
    public static final String TIMESTAMP_DAY_PATTERN = "yyyy-M-dd";
}
