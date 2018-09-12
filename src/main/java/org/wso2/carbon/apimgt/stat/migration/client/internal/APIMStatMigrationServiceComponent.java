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

package org.wso2.carbon.apimgt.stat.migration.client.internal;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.osgi.service.component.ComponentContext;
import org.wso2.carbon.apimgt.impl.APIManagerConfigurationService;
import org.wso2.carbon.apimgt.impl.utils.APIMgtDBUtil;
import org.wso2.carbon.apimgt.stat.migration.APIMStatMigrationException;
import org.wso2.carbon.apimgt.stat.migration.client.APIMStatMigrationClient;
import org.wso2.carbon.apimgt.stat.migration.client.MigrationClient;
import org.wso2.carbon.apimgt.stat.migration.util.APIMStatMigrationConstants;
import org.wso2.carbon.apimgt.stat.migration.util.DatabaseManager;

/**
 * @scr.component name="org.wso2.carbon.apimgt.stat.migration.client" immediate="true"
 * @scr.reference name="apim.configuration" interface="org.wso2.carbon.apimgt.impl.APIManagerConfigurationService" cardinality="1..1"
 * policy="dynamic" bind="setApiManagerConfig" unbind="unsetApiManagerConfig"
 */

public class APIMStatMigrationServiceComponent {
    private static final Log log = LogFactory.getLog(APIMStatMigrationServiceComponent.class);

    /**
     * Method to activate bundle.
     *
     * @param context OSGi component context.
     */
    protected void activate(ComponentContext context) {
        try {
            APIMgtDBUtil.initialize();
        } catch (Exception e) {
            //APIMgtDBUtil.initialize() throws generic exception
            log.error("Error occurred while initializing DB Util ", e);
        }

        boolean isStatMigration = Boolean.parseBoolean(System.getProperty(APIMStatMigrationConstants.ARG_MIGRATE_STATS));

        try {
            log.info("Migrating to WSO2 API Manager 2.6.0 stats DB");

            // Create a thread and wait till the APIManager DBUtils is initialized

            MigrationClient migrateStatDB = new APIMStatMigrationClient();

            if (isStatMigration) {
                DatabaseManager.initialize();
                migrateStatDB.statDbMigration();
                log.info("Stat migration completed");
            }

            if (log.isDebugEnabled()) {
                log.debug("API Manager 2.6.0 Stat migration successfully completed");
            }
        } catch (APIMStatMigrationException e) {
            log.error("API Management  exception occurred while migrating", e);
        } catch (Exception e) {
            log.error("Generic exception occurred while migrating", e);
        }
        log.info("WSO2 API Manager stat migration component successfully activated.");
    }

    /**
     * Method to deactivate bundle.
     *
     * @param context OSGi component context.
     */
    protected void deactivate(ComponentContext context) {
        log.info("WSO2 API Manager migration bundle is deactivated");
    }

    /**
     * Method to set API Manager configuration
     *
     * @param apiManagerConfig api manager configuration
     */
    protected void setApiManagerConfig(APIManagerConfigurationService apiManagerConfig) {
        log.info("Setting APIManager configuration");
    }

    /**
     * Method to unset API manager configuration
     *
     * @param apiManagerConfig api manager configuration
     */
    protected void unsetApiManagerConfig(APIManagerConfigurationService apiManagerConfig) {
        log.info("Un-setting APIManager configuration");
    }

}
