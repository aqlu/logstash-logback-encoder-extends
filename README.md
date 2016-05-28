# logstash-logback-encoder-extends

#### Contents:
* [Including it in your project](#include)
* [Usage](#usage)
  * [Kafaka Appender](#kafka)
  * [Redis Appender](#redis)
  

### <a name="include"/>Including it in your project
pom.xml

```xml
<dependency>
  <groupId>net.logstash.logback</groupId>
  <artifactId>logstash-logback-encoder</artifactId>
  <version>4.7</version>
</dependency>
```

### <a name="usage"/> Usage:

#### <a name="kafka"/> LogstashKafkaAppender
log to kafka, kafka must be 0.9 or later, and any params can be set by multiple '&lt;producerConfig&gt;key=value&lt;/producerConfig&gt;'

```xml
   <appender name="kafkaAppender" class="net.logstash.logback.appender.LogstashKafkaAppender">
       <queueSize>1024</queueSize>                      <!-- default buffer size, default is 1024 -->
       <discardingThreshold>204</discardingThreshold>   <!-- if queue remaining capacity less then this value, debug and info will be discard. default is queueSize/5 -->
       <topic>log-topic</topic>                         <!-- topic name, required -->
       <daemonThread>true</daemonThread>                <!-- set executor thread mode; default is true -->
       <maximumFrequency>1000</maximumFrequency>        <!-- maximum count of events per second, default is 1000 -->
       <ignoreOverload>false</ignoreOverload>           <!-- if true ignore overload and continue write to kafka; default is false-->
       <producerConfig>bootstrap.servers=localhost:9092</producerConfig>           <!-- kafka producer config, required -->
       <producerConfig>acks=1</producerConfig>           <!-- kafka producer config, required -->
       <layout class="net.logstash.logback.layout.LogstashLayout">
           <includeContext>false</includeContext>
           <includeMdc>false</includeMdc>
           <includeCallerInfo>false</includeCallerInfo>
           <customFields>{"source": "your source"}</customFields>
       </layout>
       <appender-ref ref="fileAppender" />                    <!-- output to this appender when output to kafka failed or output overload; if you are not configured, then ignore this log, optional; -->
   </appender>
  
   <appender name="fileAppender" class="ch.qos.logback.core.rolling.RollingFileAppender"<
       <file>/you/log/path/log-name.ing</file<
       <rollingPolicy class="ch.qos.logback.core.rolling.TimeBasedRollingPolicy">
           <fileNamePattern>/you/log/path/log-name.%d{yyyy-MM-dd_HHmm}.log.%i</fileNamePattern>
           <MaxHistory>30</MaxHistory>
       </rollingPolicy>
       <encoder class="net.logstash.logback.encoder.LogstashEncoder">
           <includeContext>false</includeContext>
           <includeMdc>false</includeMdc>
           <includeCallerInfo>false</includeCallerInfo>
           <customFields>{"source": "your source", "type": "your type"}</customFields>
       </encoder>
   </appender>
```

#### <a name="redis"/>LogstashRedisAppender

```
 <appender name="redisAppender" class="net.logstash.logback.appender.LogstashRedisAppender">
       <queueSize>1024</queueSize>                      <!-- default buffer size, default is 1024 -->
       <discardingThreshold>204</discardingThreshold>   <!-- if queue remaining capacity less then this value, debug and info will be discard. default is queueSize/5 -->
       <host>172.19.65.153</host>                       <!-- redis host, required -->
       <port>6379</port>                                <!-- redis port, default is 6379 -->
       <database>0</database>                           <!-- redis database, default is 0 -->
       <password></password>                            <!-- redis password, optional -->
       <maxIdle>1</maxIdle>                             <!-- redis connect for maxIdle, default is 1 -->
       <maxTotal>1</maxTotal>                           <!-- redis connect for maxTotal, default is 1 -->
       <maxWaitMills>1000</maxWaitMills>                <!-- max wait(Mills) for get redis connection, default 1000 -->
       <timeout>2000</timeout>                          <!-- redis timeout, default is 2000ms -->
       <key>logstash_intf_log</key>                     <!-- redis key, required -->
       <daemonThread>true</daemonThread>                <!-- set executor thread mode; default is true -->
       <batchSize>100</batchSize>                       <!-- batch size, default 100 -->
       <period>500</period>                             <!-- each write period redis, default 500 ms -->
       <maximumFrequency>10000</maximumFrequency>       <!-- maximum count of events per second, default is 10000 -->
       <ignoreOverload>false</ignoreOverload>           <!-- if true ignore overload and continue write to redis; default is false-->
       <layout class="net.logstash.logback.layout.LogstashLayout">
           <includeContext>false</includeContext>
           <includeMdc>false</includeMdc>
           <includeCallerInfo>false</includeCallerInfo>
           <customFields>{"source": "your source"}</customFields>
       </layout>
       <appender-ref ref="fileAppender" />                    <!-- output to this appender when output to redis failed or output overload; if you are not configured, then ignore this log, optional; -->
   </appender>
```