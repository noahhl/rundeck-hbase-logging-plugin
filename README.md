# Rundeck HBase Loggin Plugin

This is an [Execution Logging Plugin](http://rundeck.org/docs/developer/logging-plugin.html#executionfilestorage) for [Rundeck](http://rundeck.org) that stores execution log files in [HBase](http://hbase.apache.org).

## Build
  
```
./gradlew build
```

## Hbase setup

Create an HBase table naemd "rundeck" with a "logs" column family. You'll also
need to be running the HBase Thrift API.

## Install

Copy the `rundeck-hbase-logging-plugin-a.b.c.jar` file to the `libext/`
directory of your Rundeck installation. Copy the
`rundeck-hbase-logging-plugin-dependencies-a.b.c.jar` file to somewhere in
Rundeck's classpath.

Enable the `ExecutionFileStorageProvider` in your rundeck-config file:

```
rundeck.execution.logs.fileStoragePlugin=hbase
```

Configure the hostname and port of your HBase Thrift installation in your
framework.properties file:

```
framework.plugin.ExecutionFileStorage.hbase.hostname=1.2.3.4
framework.plugin.ExecutionFileStorage.hbase.port=1234
```


