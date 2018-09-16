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

import org.wso2.carbon.apimgt.stat.migration.APIMStatMigrationException;

public interface DBManager {

    void initialize() throws APIMStatMigrationException;

    void migrateDestinationSummaryTable() throws APIMStatMigrationException;

    void migrateResourceUsageSummaryTable() throws APIMStatMigrationException;

    void migrateVersionUsageSummaryTable() throws APIMStatMigrationException;

    void migrateLastAccessTimeSummaryTable() throws APIMStatMigrationException;

    void migrateFaultSummaryTable() throws APIMStatMigrationException;

    void migrateUserBrowserSummaryTable() throws APIMStatMigrationException;

    void migrateGeoLocationSummaryTable() throws APIMStatMigrationException;

    void migrateExecutionTimeDaySummaryTable() throws APIMStatMigrationException;

    void migrateExecutionTimeHourSummaryTable() throws APIMStatMigrationException;

    void migrateExecutionTimeMinuteSummaryTable() throws APIMStatMigrationException;

    void migrateThrottledOutSummaryTable() throws APIMStatMigrationException;

    void migrateThrottledOutRequestCountSummaryTable() throws APIMStatMigrationException;

    void migrateRequestSummaryTable() throws APIMStatMigrationException;
}

