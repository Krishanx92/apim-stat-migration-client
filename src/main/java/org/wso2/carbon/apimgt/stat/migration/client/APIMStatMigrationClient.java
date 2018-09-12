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

package org.wso2.carbon.apimgt.stat.migration.client;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.carbon.apimgt.stat.migration.APIMStatMigrationException;
import org.wso2.carbon.apimgt.stat.migration.util.DatabaseManager;

public class APIMStatMigrationClient implements MigrationClient {

    private static final Log log = LogFactory.getLog(APIMStatMigrationClient.class);

    @Override
    public void statDbMigration() throws APIMStatMigrationException {
        log.info("Started stat db migration......");
        DatabaseManager.migrateDestinationSummaryTable();
        log.info("Completed stat db migration successfully.....");
    }
}