/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.logstash.logback.appender;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;


import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.UnsynchronizedAppenderBase;
import ch.qos.logback.core.spi.AppenderAttachable;
import ch.qos.logback.core.spi.AppenderAttachableImpl;
import net.logstash.logback.layout.LogstashLayout;
import net.logstash.logback.util.NamedThreadFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Protocol;

/**
 * Created by aqlu on 2014-10-13.
 * output log to redis
 * <p>
 * Configurations example:
 * 
 * <pre>
 * &lt;appender name="redisAppender" class="net.logstash.logback.appender.LogstashRedisAppender"&gt;
 *     &lt;queueSize&gt;1024&lt;/queueSize&gt;                      &lt;!-- default buffer size, default is 1024 --&gt;
 *     &lt;discardingThreshold&gt;204&lt;/discardingThreshold&gt;   &lt;!-- if queue remaining capacity less then this value, debug and info will be discard. default is queueSize/5 --&gt;
 *     &lt;host&gt;172.19.65.153&lt;/host&gt;                       &lt;!-- redis host, required --&gt;
 *     &lt;port&gt;6379&lt;/port&gt;                                &lt;!-- redis port, default is 6379 --&gt;
 *     &lt;database&gt;0&lt;/database&gt;                           &lt;!-- redis database, default is 0 --&gt;
 *     &lt;password&gt;&lt;/password&gt;                            &lt;!-- redis password, optional --&gt;
 *     &lt;maxIdle&gt;1&lt;/maxIdle&gt;                             &lt;!-- redis connect for maxIdle, default is 1 --&gt;
 *     &lt;maxTotal&gt;1&lt;/maxTotal&gt;                           &lt;!-- redis connect for maxTotal, default is 1 --&gt;
 *     &lt;maxWaitMills&gt;1000&lt;/maxWaitMills&gt;                &lt;!-- max wait(Mills) for get redis connection, default 1000 --&gt;
 *     &lt;timeout&gt;2000&lt;/timeout&gt;                          &lt;!-- redis timeout, default is 2000ms --&gt;
 *     &lt;key&gt;logstash_intf_log&lt;/key&gt;                     &lt;!-- redis key, required --&gt;
 *     &lt;daemonThread&gt;true&lt;/daemonThread&gt;                &lt;!-- set executor thread mode; default is true --&gt;
 *     &lt;batchSize&gt;100&lt;/batchSize&gt;                       &lt;!-- batch size, default 100 --&gt;
 *     &lt;period&gt;500&lt;/period&gt;                             &lt;!-- each write period redis, default 500 ms --&gt;
 *     &lt;maximumFrequency&gt;10000&lt;/maximumFrequency&gt;       &lt;!-- maximum count of events per second, default is 10000 --&gt;
 *     &lt;ignoreOverload&gt;false&lt;/ignoreOverload&gt;           &lt;!-- if true ignore overload and continue write to redis; default is false--&gt;
 *     &lt;layout class="net.logstash.logback.layout.LogstashLayout"&gt;
 *         &lt;includeContext&gt;false&lt;/includeContext&gt;
 *         &lt;includeMdc&gt;false&lt;/includeMdc&gt;
 *         &lt;includeCallerInfo&gt;false&lt;/includeCallerInfo&gt;
 *         &lt;customFields&gt;{"source": "your source"}&lt;/customFields&gt;
 *     &lt;/layout&gt;
 *     &lt;appender-ref ref="fileAppender" /&gt;                    &lt;!-- output to this appender when output to redis failed or output overload; if you are not configured, then ignore this log, optional; --&gt;
 * &lt;/appender&gt;
 * 
 * &lt;appender name="fileAppender" class="ch.qos.logback.core.rolling.RollingFileAppender"&lt;
 *     &lt;file&gt;/you/log/path/log-name.ing&lt;/file&lt;
 *     &lt;rollingPolicy class="ch.qos.logback.core.rolling.TimeBasedRollingPolicy"&gt;
 *         &lt;fileNamePattern&gt;/you/log/path/log-name.%d{yyyy-MM-dd_HHmm}.log.%i&lt;/fileNamePattern&gt;
 *         &lt;MaxHistory&gt;30&lt;/MaxHistory&gt;
 *     &lt;/rollingPolicy&gt;
 *     &lt;encoder class="net.logstash.logback.encoder.LogstashEncoder"&gt;
 *         &lt;includeContext&gt;false&lt;/includeContext&gt;
 *         &lt;includeMdc&gt;false&lt;/includeMdc&gt;
 *         &lt;includeCallerInfo&gt;false&lt;/includeCallerInfo&gt;
 *         &lt;customFields&gt;{"source": "your source", "type": "your type"}&lt;/customFields&gt;
 *     &lt;/encoder&gt;
 * &lt;/appender&gt;
 * </pre>
 * 
 * </p>
 */
@SuppressWarnings("unused")
public class LogstashRedisAppender extends UnsynchronizedAppenderBase<ILoggingEvent> implements
        AppenderAttachable<ILoggingEvent>, Runnable {

    private static final int DEFAULT_QUEUE_SIZE = 1024;

    private static final int DEFAULT_MAX_IDLE = 1;

    private static final int DEFAULT_MAX_TOTAL = 1;

    private static final int DEFAULT_MAX_WAIT_MILLIS = 1000;

    private static final int UNDEFINED = -1;

    private AtomicInteger eventsPerSecond = new AtomicInteger(0);

    private AppenderAttachableImpl<ILoggingEvent> aai = new AppenderAttachableImpl<>();

    private BlockingQueue<ILoggingEvent> blockingQueue;

    private LogstashLayout layout = new LogstashLayout();

    private ExecutorService executor;

    private ScheduledExecutorService cleanExecutor;

    private int appenderCount = 0;

    private JedisPool pool;

    /* configurable params */

    private int queueSize = DEFAULT_QUEUE_SIZE;

    private String host;

    private int port = Protocol.DEFAULT_PORT;

    private int database = Protocol.DEFAULT_DATABASE;

    private int maxIdle = DEFAULT_MAX_IDLE;

    private int maxTotal = DEFAULT_MAX_TOTAL;

    private int maxWaitMills = DEFAULT_MAX_WAIT_MILLIS;

    private String key;

    private int timeout = Protocol.DEFAULT_TIMEOUT;

    private String password;

    private int discardingThreshold = UNDEFINED;

    private int batchSize = 100;

    private long period = 500; // ms

    private boolean daemonThread = true;

    private boolean ignoreOverload = false;

    private int maximumFrequency = 10000;

    public void run() {
        List<String> logList = new ArrayList<>(batchSize);
        List<ILoggingEvent> eventList = new ArrayList<>(batchSize);

        while(this.isStarted()){
            try {
                ILoggingEvent event = this.blockingQueue.poll(period, TimeUnit.MILLISECONDS);
                if(event != null) {
                    logList.add(layout.doLayout(event));
                    eventList.add(event);

                    if (logList.size() >= batchSize) {
                        push(logList, eventList);
                    }
                }else if(logList.size() > 0) {
                    push(logList, eventList);
                }
            } catch (Exception ie) {
                if(logList.size() > 0) {
                    push(logList, eventList);
                }
                break;
            }
        }

        addInfo("Worker thread will flush remaining events before exiting. ");

        for (ILoggingEvent event : this.blockingQueue) {
            logList.add(layout.doLayout(event));
            eventList.add(event);
        }

        if(logList.size() > 0 ){
            push(logList, eventList);
        }

        aai.detachAndStopAllAppenders();
    }

    @Override
    @SuppressWarnings("Duplicates")
    protected void append(ILoggingEvent event) {
        try {
            if (isQueueBelowDiscardingThreshold() && isDiscardable(event)) {
                return;
            }

            event.prepareForDeferredProcessing();

            if(layout.isIncludeCallerData()) {
                event.getCallerData();
            }

            blockingQueue.put(event);

        } catch (InterruptedException ignored) {

        }
    }

    /**
     * push log to redis
     * @param jsonLogs logs for json format
     */
    private void push(List<String> jsonLogs, List<ILoggingEvent> eventList) {
        Jedis client = null;

        try {
            if(ignoreOverload || eventsPerSecond.addAndGet(jsonLogs.size()) < maximumFrequency) {
                client = pool.getResource();
                client.rpush(key, jsonLogs.toArray(new String[jsonLogs.size()]));
            }else {
                if (appenderCount > 0) {
                    this.appendLoopOnAppends(eventList.toArray(new ILoggingEvent[eventList.size()]));
                }else{
                    addWarn("append event overload, Current frequency is " + eventsPerSecond.get() + " per second");
                }
            }
        } catch (Exception e) {
            if (client != null) {
                //noinspection deprecation
                pool.returnBrokenResource(client);
                client = null;
            }

            addInfo("record log to redis failed." + e.getMessage());
            if (appenderCount > 0) {
                this.appendLoopOnAppends(eventList.toArray(new ILoggingEvent[eventList.size()]));
            }
        } finally {
            if (client != null) {
                //noinspection deprecation
                pool.returnResource(client);
            }

            jsonLogs.clear();
            eventList.clear();
        }
    }

    private void appendLoopOnAppends(ILoggingEvent... events) {
        for (ILoggingEvent event : events) {
            this.aai.appendLoopOnAppenders(event);
        }
    }

    @Override
    public void start() {
        super.start();
        blockingQueue = new ArrayBlockingQueue<>(queueSize);

        if (discardingThreshold == UNDEFINED)
            discardingThreshold = queueSize / 5;
        addInfo("Setting discardingThreshold to " + discardingThreshold);

        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setJmxEnabled(true);
        jedisPoolConfig.setJmxNamePrefix(this.getName() + "-redis-pool");
        jedisPoolConfig.setMaxIdle(maxIdle);
        jedisPoolConfig.setMaxTotal(maxTotal);
        jedisPoolConfig.setMaxWaitMillis(maxWaitMills);
        pool = new JedisPool(jedisPoolConfig, host, port, timeout, password, database);

        executor = Executors.newSingleThreadExecutor(new NamedThreadFactory(getName() + "-RedisAppender", daemonThread));
        executor.submit(this);
        cleanExecutor = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory(getName() + "-frequency-clean-thread", daemonThread));
        cleanExecutor.scheduleWithFixedDelay(new Runnable() {
            public void run() {
                eventsPerSecond.set(0);
            }
        }, 1, 1, TimeUnit.SECONDS);

    }

    @Override
    public void stop() {
        super.stop();
        executor.shutdown();
        cleanExecutor.shutdown();
        pool.destroy();
    }

    private boolean isDiscardable(ILoggingEvent event) {
        Level level = event.getLevel();
        return level.toInt() <= Level.INFO_INT;
    }

    private boolean isQueueBelowDiscardingThreshold() {
        return (blockingQueue.remainingCapacity() < discardingThreshold);
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public int getDatabase() {
        return database;
    }

    public void setDatabase(int database) {
        this.database = database;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public int getTimeout() {
        return timeout;
    }

    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public LogstashLayout getLayout() {
        return layout;
    }

    public void setLayout(LogstashLayout layout) {
        this.layout = layout;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public long getPeriod() {
        return period;
    }

    public void setPeriod(long period) {
        this.period = period;
    }

    public boolean isDaemonThread() {
        return daemonThread;
    }

    public void setDaemonThread(boolean daemonThread) {
        this.daemonThread = daemonThread;
    }

    public int getMaximumFrequency() {
        return maximumFrequency;
    }

    public void setMaximumFrequency(int maximumFrequency) {
        this.maximumFrequency = maximumFrequency;
    }

    public boolean isIgnoreOverload() {
        return ignoreOverload;
    }

    public void setIgnoreOverload(boolean ignoreOverload) {
        this.ignoreOverload = ignoreOverload;
    }

    public int getDiscardingThreshold() {
        return discardingThreshold;
    }

    public void setDiscardingThreshold(int discardingThreshold) {
        this.discardingThreshold = discardingThreshold;
    }

    public int getMaxWaitMills() {
        return maxWaitMills;
    }

    public void setMaxWaitMills(int maxWaitMills) {
        this.maxWaitMills = maxWaitMills;
    }

    public int getMaxTotal() {
        return maxTotal;
    }

    public void setMaxTotal(int maxTotal) {
        this.maxTotal = maxTotal;
    }

    public int getMaxIdle() {
        return maxIdle;
    }

    public void setMaxIdle(int maxIdle) {
        this.maxIdle = maxIdle;
    }

    public int getQueueSize() {
        return queueSize;
    }

    public void setQueueSize(int queueSize) {
        this.queueSize = queueSize;
    }

    public void addAppender(Appender<ILoggingEvent> newAppender) {
        appenderCount++;
        addInfo("Attaching appender named [" + newAppender.getName() + "] to LogstashRedisAppender.");
        aai.addAppender(newAppender);
    }

    public Iterator<Appender<ILoggingEvent>> iteratorForAppenders() {
        return aai.iteratorForAppenders();
    }

    public Appender<ILoggingEvent> getAppender(String name) {
        return aai.getAppender(name);
    }

    public boolean isAttached(Appender<ILoggingEvent> appender) {
        return aai.isAttached(appender);
    }

    public void detachAndStopAllAppenders() {
        aai.detachAndStopAllAppenders();
    }

    public boolean detachAppender(Appender<ILoggingEvent> appender) {
        return aai.detachAppender(appender);
    }

    public boolean detachAppender(String name) {
        return aai.detachAppender(name);
    }
}
