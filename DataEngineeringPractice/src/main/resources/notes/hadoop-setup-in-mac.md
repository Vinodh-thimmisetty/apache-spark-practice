[Download Hadoop](https://www.apache.org/dyn/closer.cgi/hadoop/common/hadoop-3.3.4/hadoop-3.3.4.tar.gz)  
[Download Hive](https://dlcdn.apache.org/hive/hive-3.1.3/)

[LOCAL HDFS URL](http://localhost:9870/)  
[LOCAL YARN URL](http://localhost:8088/cluster)

```shell
vim ~/.zshrc

#Java
export JAVA_HOME=`/usr/libexec/java_home -v 1.8.0_352`

#Hadoop
export HADOOP_HOME=~/BigDataTools/hadoop-3.3.4
export HADOOP_MAPRED_HOME=$HADOOP_HOME
export HADOOP_COMMON_HOME=$HADOOP_HOME
export HADOOP_HDFS_HOME=$HADOOP_HOME
export YARN_HOME=$HADOOP_HOME
export HADOOP_COMMON_LIB_NATIVE_DIR=$HADOOP_HOME/lib/native export
export PATH=$PATH:$HADOOP_HOME/sbin:$HADOOP_HOME/bin

#Hive
export HIVE_HOME=~/BigDataTools/apache-hive-3.1.3-bin
export PATH=$HIVE_HOME/bin:$PATH
```

```xml
<!--$HADOOP_HOME/etc/hadoop/core-site.xml-->
<configuration>
    <property>
        <name>fs.default.name</name>
        <value>hdfs://localhost:9000</value>
    </property>
</configuration>
```

```xml
<!--$HADOOP_HOME/etc/hadoop/hdfs-site.xml-->
<configuration>
    <property>
        <name>dfs.replication</name>
        <value>1</value>
    </property>
    <property>
        <name>dfs.name.dir</name>
        <value>file://~/BigDataTools/hdfs/namenode</value>
    </property>
    <property>
        <name>dfs.data.dir</name>
        <value>file://~/BigDataTools/hdfs/datanode</value>
    </property>
</configuration>
```

```xml
<!--$HADOOP_HOME/etc/hadoop/yarn-site.xml-->
<configuration>
    <property>
        <name>yarn.nodemanager.aux-services</name>
        <value>mapreduce_shuffle</value>
    </property>
    <property>
        <name>yarn.application.classpath</name>
        <value>/Full_File_path/hadoop-3.3.4/share/hadoop/common/*,/Full_File_path/hadoop-3.3.4/share/hadoop/common/lib/*,/Full_File_path/hadoop-3.3.4/share/hadoop/hdfs/*,/Full_File_path/hadoop-3.3.4/share/hadoop/hdfs/lib/*,/Full_File_path/hadoop-3.3.4/share/hadoop/mapreduce/*,/Full_File_path/hadoop-3.3.4/share/hadoop/mapreduce/lib/*,/Full_File_path/hadoop-3.3.4/share/hadoop/yarn/*,/Full_File_path/hadoop-3.3.4/share/hadoop/yarn/lib/*</value>
    </property>
</configuration>
```

```xml
<!--$HADOOP_HOME/etc/hadoop/mapred-site.xml-->
<configuration>
    <property>
        <name>mapreduce.framework.name</name>
        <value>yarn</value>
    </property>
    <property>
        <name>yarn.app.mapreduce.am.env</name>
        <value>HADOOP_MAPRED_HOME=/Full_File_path/hadoop-3.3.4</value>
    </property>
    <property>
        <name>mapreduce.map.env</name>
        <value>HADOOP_MAPRED_HOME=/Full_File_path/hadoop-3.3.4</value>
    </property>
    <property>
        <name>mapreduce.reduce.env</name>
        <value>HADOOP_MAPRED_HOME=/Full_File_path/hadoop-3.3.4</value>
    </property>
    <property>
        <name>mapreduce.application.classpath</name>
               <value>/Full_File_path/hadoop-3.3.4/share/hadoop/common/*,/Full_File_path/hadoop-3.3.4/share/hadoop/common/lib/*,/Full_File_path/hadoop-3.3.4/share/hadoop/hdfs/*,/Full_File_path/hadoop-3.3.4/share/hadoop/hdfs/lib/*,/Full_File_path/hadoop-3.3.4/share/hadoop/mapreduce/*,/Full_File_path/hadoop-3.3.4/share/hadoop/mapreduce/lib/*,/Full_File_path/hadoop-3.3.4/share/hadoop/yarn/*,/Full_File_path/hadoop-3.3.4/share/hadoop/yarn/lib/*</value>
    </property>
</configuration>
```

```xml

<!--$HIVE_HOME/apache-hive-3.1.3-bin/conf/hive-site.xml-->
<configuration>
    <property>
        <name>hive.server2.enable.doAs</name>
        <value>FALSE</value>
        <description>
            Setting this property to true will have HiveServer2 execute
            Hive operations as the user making the calls to it.
        </description>
    </property>
    <property>
        <name>javax.jdo.option.ConnectionURL</name>
        <value>jdbc:mysql://localhost:3306/metastore?createDatabaseIfNotExist=true</value>
    </property>
    <property>
        <name>javax.jdo.option.ConnectionUserName</name>
        <value>mysqlUserName</value>
    </property>
    <property>
        <name>javax.jdo.option.ConnectionPassword</name>
        <value>mysqlPwd</value>
    </property>
    <property>
        <name>javax.jdo.option.ConnectionDriverName</name>
        <value>com.mysql.cj.jdbc.Driver</value>
    </property>

</configuration>
```
