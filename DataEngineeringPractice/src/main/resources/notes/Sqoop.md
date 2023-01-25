[Free Online MySQL Setup](https://www.freesqldatabase.com/account/)

```textmate
Key Points:

Data can be imported from Sql to Hadoop/Hive/Hbase, but only supports export from Hadoop/Hive to Database.
Supports both HDFS or S3 storages
Map only Job (Reducer won't come to play as we don't need any shuffle or aggregation)
```

```shell
Basic Commands to check db connectivity

sqoop list-databases --connect jdbc:<db-type>://<jdbc-host-url>:<port> --username ***** --password ****
sqoop list-tables --connect jdbc:<db-type>://<jdbc-host-url>/<database-or-schema-name>:<port> --username ***** --password ****
sqoop eval --connect jdbc:<db-type>://<jdbc-host-url>/<database-or-schema-name>:<port> --username ***** --password **** --query "select * from table where condition = true"
```

```shell
Simple Import from DB to Hadoop

sqoop import 
      --connect JDBC_URL
      --driver DriverName
      --username DBName
      --password ***
      --table TableName
      --where filterClause
      --split-by partitionColumn #use this in conjunction with --table option when data split is not based on Primary Key
      --null-string ""
      --null-non-string -1
      --verbose #Add more logging
      --direct #Some DBs like Mysql/Postgres/Oracle support direct data movement without Map Reduce
      --fs s3a #File System URI for target Storage System
      --delete-target-dir # MR Job fails if output directory already exists
      --target-dir /hdfs-or-s3-location
      --num-mappers N (or) -m #No. of Mappers- this decides how many parallel tasks
      --fetch-size X #Number of rows read per mapper
      -z #compression(default is gzip)
      --compression-codec org.apache.hadoop.io.compress.SnappyCodec #In-case you want other than gzip
```

```shell
Import based on Joining Multiple Tables or Import only some columns in a table

      --query "select * from table WHERE \$CONDITIONS" #uses PK to split the data across mappers
      --query "select * from table WHERE start > 1 and end < 100 and \$CONDITIONS" #uses start and end as Bounded Query
  
    1. $CONDITIONS act as bounded query to read metadata like columns and total rows which helps in splitting up the data across mappers.
    2. When -m 1 then no parallelism  is applied for import ( use it when you know data is small enough to process in SINGLE mapper)
    3. Enable the --verbose mode to see pre-action queries it runs before original query starts pulling the data.
```

```shell
Destination is Hive Table(Internal)
    --hive-import
    --create-hive-table
    --hive-table <hive_table>
    --warehouse-dir </user/hive/warehouse/>hive_table_name

To convert Internal Hive table to External :- [Alter table <hive_table_name> SET TBLPROPERTIES('EXTERNAL'='TRUE')]

Best thing is creating Hive tables(internal or external) before sqoop and use normal sqoop import pointing target-directory to Hive Location
```

```shell
Multi Table Import

sqoop import-all-tables #All tables must have PK
      --exclude-tables tableToExclude #Ignore some tables
      --warehouse-dir /hdfs-or-s3/path/ #Destination folder
      --options-file sqoop-options.properties #we can add all our common arguments like DB connection details into file
```

```shell
Incremental Import

    --incremental append/lastmodified
    --check-column numeric_column/date_column
    --last-value $envVariableHoldingLastImportValue #max(column) from existing data
    
To automatically get max(column) from last import, we have to run separate action and store it as configuration parameter or environmental variable
```

```text
How to secure your Password?
    1. Store your DB Passwords in some KeyVault or Password Manager like aws-secrets-manager
    2. During Cluster Setup, read this keys, encrypt and store in some common HDFS Location like /user/hadoop
    3. Configure encrypted provider details in sqoop-site.xml or similar configs
    4. use --password-alias instead of --password as part of sqoop import jobs

Validation Steps:
1. hadoop credential create sqoop-mysql.password -provider jceks://hdfs/user/hadoop/sqoop-mysql.jceks
2. hadoop credential list -provider jceks://hdfs/user/hadoop/sqoop-mysql.jceks
3. validate the sqoop.connection.credstore.providerpath key in sqoop-site.xml
        or
   Pass -Dhadoop.security.credential.provider.path=jceks://hdfs/user/ec2-user/sqoop-mysql.jceks argument to every sqoop command
    
Note: Other less secure way is directing storing the DB credentials in a text file and store in hdfs location and read like: 
    --password-file /hdfs/.creds/mysql_creds.txt
```

```xml
<!-- oozie setup-->

<workflow-app name="sqoop-jobs">
   <global>
       <job-tracker>${resourceManager}</job-tracker>
       <name-node>${nameNode}</name-node>
      <configuration>
            <name>some_key</name>
            <value>some_value</value>
      </configuration>
   </global>
   <start name="sqoop-import-with-command" />
   <action name="sqoop-import-with-command">
      <sqoop>
         <configuration />
         <prepare>
            <delete path="" />
            <mkdir path="" />
         </prepare>
         <command>
          import --connect URL --username DBName --password-alias encyptedPwd --table TableName --where filterClause --split-by partitionColumn --num-mappers X --options-file sqoop-with-command.properties
         </command> 
         <file>
            somePath/sqoop-mysql.properties#sqoop-with-command.properties
              **** sqoop-mysql.properties -> is the filename in workflow
              **** sqoop-with-command.properties --> is the filename to use inside action
         </file>
      </sqoop>
         <ok to="sqoop-import-with-arg"/>
         <error to="error"/>
   </action>
   <action name="sqoop-import-with-arg">
      <sqoop>
          <configuration>
          </configuration>
         <prepare>
            <delete />
            <mkdir />
         </prepare>
         <arg>import</arg>
         <arg>--connect</arg>
         <arg>jdbc:hsqldb:file:db.hsqldb</arg>
         <arg>--table</arg>
         <arg>TT</arg>
         <arg>--target-dir</arg>
         <arg>hdfs://localhost:8020/user/tucu/foo</arg>
         <arg>-m</arg>
         <arg>1</arg>
         <file>
            somePath/sqoop-mysql.properties#sqoop-mysql.properties
         </file>
      </sqoop>
         <ok to="end"/>
         <error to="error"/>
   </action>
   <kill name="error">
    <message>error message[${wf:errorMessage(wf:lastErrorNode())}]</message>
   </kill>
   <end name="end"/>
</workflow-app>
```

```textmate
Special Cases

If Partition value has special characters and if import fails then follow 3 step approach
1. Create Partitioned External Hive table with STORED as parquet format or any other desired format
2. Import data --as-parquetfile to some temp location without partitioned column in path
3. Move the data from temp to original location and Remove the temp folders
```
