# WSO2 API Manager Stat Migration Client
This is used to migrate the DAS based statistics DB to SP from WSO2 API Manager 2.6.0

### Follow the steps below
1. Create a new database for the new statistics database and configure it in `<NEW_APIM_ANALYTICS_HOME>/conf/worker/deployment.yaml` as follows.
```yaml
- name: APIM_ANALYTICS_DB
      description: "The datasource used for APIM statistics aggregated data."
      jndiConfig:
        name: jdbc/APIM_ANALYTICS_DB
      definition:
        type: RDBMS
        configuration:
          jdbcUrl: 'jdbc:mysql://localhost:3306/spDatabase?autoReconnect=true'
          username: username
          password: password
          driverClassName: com.mysql.jdbc.Driver
          maxPoolSize: 50
          idleTimeout: 60000
          connectionTestQuery: SELECT 1
          validationTimeout: 30000
          isAutoCommit: false
```
2. Copy the relevant jdbc driver to `<NEW_APIM_ANALYTICS_HOME>/lib` folder.

3. Startup the new analytics server with the following commands in the command prompt.
```shell
$ cd <NEW_APIM_ANALYTICS_HOME>/bin
$ sh worker.sh
```
With the start up of the server, the new statistics tables will get created in the database configured in step 1 above.

4. The following 3 datasources should be configured in `<NEW_APIM_HOME>/repository/conf/datasources/master-datasources.xml` only until the stats migration is complete. After that these configurations should be reverted to the default values. Also remove the datasource configuration `APIM_ANALYTICS_DB` for the new statistics database from this file.
  - The first datasource points to the previous APIM version's AM_DB datasource
  - The second datasource points to the DAS based previous datasource for statistics
  - The third datasource points to the SP based new datasource for statistics

```xml
    <datasource>
            <name>WSO2AM_DB</name>
            <description>The datasource used for API Manager database</description>
            <jndiConfig>
                <name>jdbc/WSO2AM_DB</name>
            </jndiConfig>
            <definition type="RDBMS">
                <configuration>
                    <url>jdbc:mysql://localhost:3306/AM_DB?autoReconnect=true</url>
                    <username>username</username>
                    <password>password</password>
                    <defaultAutoCommit>true</defaultAutoCommit>
                    <driverClassName>com.mysql.jdbc.Driver</driverClassName>
                    <maxActive>50</maxActive>
                    <maxWait>60000</maxWait>
                    <testOnBorrow>true</testOnBorrow>
                    <validationQuery>SELECT 1</validationQuery>
                    <validationInterval>30000</validationInterval>
                </configuration>
            </definition>
        </datasource>

         <datasource>
            <name>WSO2AM_STATS_DB</name>
            <description>The datasource used for getting statistics to API Manager</description>
            <jndiConfig>
                <name>jdbc/WSO2AM_STATS_DB</name>
            </jndiConfig>
            <definition type="RDBMS">
                <configuration>
                    <url>jdbc:mysql://localhost:3306/dasDatabase?autoReconnect=true</url>
                    <username>username</username>
                    <password>password</password>
                    <defaultAutoCommit>true</defaultAutoCommit>
                    <driverClassName>com.mysql.jdbc.Driver</driverClassName>
                    <maxActive>50</maxActive>
                    <maxWait>60000</maxWait>
                    <testOnBorrow>true</testOnBorrow>
                    <validationQuery>SELECT 1</validationQuery>
                    <validationInterval>30000</validationInterval>
                </configuration>
            </definition>
         </datasource>

         <datasource>
            <name>APIM_ANALYTICS_DB</name>
            <description>The datasource used for getting statistics to API Manager for APIM 2.6.0</description>
            <jndiConfig>
                <name>jdbc/APIM_ANALYTICS_DB</name>
            </jndiConfig>
            <definition type="RDBMS">
                <configuration>
                    <url>jdbc:mysql://localhost:3306/spDatabase?autoReconnect=true</url>
                    <username>username</username>
                    <password>password</password>
                    <defaultAutoCommit>true</defaultAutoCommit>
                    <driverClassName>com.mysql.jdbc.Driver</driverClassName>
                    <maxActive>50</maxActive>
                    <maxWait>60000</maxWait>
                    <testOnBorrow>true</testOnBorrow>
                    <validationQuery>SELECT 1</validationQuery>
                    <validationInterval>30000</validationInterval>
                </configuration>
            </definition>
         </datasource>
```
5. Copy the jar file created by executing `mvn clean install` on this repository to `<NEW_APIM_HOME>/repository/components/dropins` folder.

6. Copy the relevant jdbc driver to `<NEW_APIM_HOME>/repository/components/lib` folder.

7. After setting the above configurations in place, start up the APIM 2.6.0 server with the following commands. Do not enable analytics in `<NEW_APIM_HOME>/repository/conf/api-manager.xml` before executing this step.
```shell
$ cd <NEW_APIM_HOME>/bin
$ sh wso2server.sh -DmigrateStats=true
```

8. Stop the server and remove the migration jar copied under step 4 above.

9. Then for normal operations you can restart the server after enabling analytics and without the option `-DmigrateStats=true`.