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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
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
import org.apache.kafka.clients.producer.*;

/**
 * output log to kafka 0.9
 * <p>
 * Configurations example:
 *
 * <pre>
 * &lt;appender name="kafkaAppender" class="net.logstash.logback.appender.LogstashKafkaAppender"&gt;
 *     &lt;queueSize&gt;1024&lt;/queueSize&gt;                      &lt;!-- default buffer size, default is 1024 --&gt;
 *     &lt;discardingThreshold&gt;204&lt;/discardingThreshold&gt;   &lt;!-- if queue remaining capacity less then this value, debug and info will be discard. default is queueSize/5 --&gt;
 *     &lt;topic&gt;log-topic&lt;/topic&gt;                         &lt;!-- topic name, required --&gt;
 *     &lt;daemonThread&gt;true&lt;/daemonThread&gt;                &lt;!-- set executor thread mode; default is true --&gt;
 *     &lt;maximumFrequency&gt;1000&lt;/maximumFrequency&gt;        &lt;!-- maximum count of events per second, default is 1000 --&gt;
 *     &lt;ignoreOverload&gt;false&lt;/ignoreOverload&gt;           &lt;!-- if true ignore overload and continue write to kafka; default is false--&gt;
 *     &lt;producerConfig&gt;bootstrap.servers=localhost:9092&lt;/producerConfig&gt;           &lt;!-- kafka producer config, required --&gt;
 *     &lt;producerConfig&gt;acks=1&lt;/producerConfig&gt;           &lt;!-- kafka producer config, required --&gt;
 *     &lt;layout class="net.logstash.logback.layout.LogstashLayout"&gt;
 *         &lt;includeContext&gt;false&lt;/includeContext&gt;
 *         &lt;includeMdc&gt;false&lt;/includeMdc&gt;
 *         &lt;includeCallerInfo&gt;false&lt;/includeCallerInfo&gt;
 *         &lt;customFields&gt;{"source": "your source"}&lt;/customFields&gt;
 *     &lt;/layout&gt;
 *     &lt;appender-ref ref="fileAppender" /&gt;                    &lt;!-- output to this appender when output to kafka failed or output overload; if you are not configured, then ignore this log, optional; --&gt;
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
 * Created by aqlu on 2016-5-3.
 */
@SuppressWarnings("unused")
public class LogstashKafkaAppender extends UnsynchronizedAppenderBase<ILoggingEvent>
        implements AppenderAttachable<ILoggingEvent>, Runnable {

    private static final int DEFAULT_QUEUE_SIZE = 1024;

    private static final int UNDEFINED = -1;

    private AtomicInteger eventsPerSecond = new AtomicInteger(0);

    private AppenderAttachableImpl<ILoggingEvent> aai = new AppenderAttachableImpl<>();

    private BlockingQueue<ILoggingEvent> blockingQueue;

    private LogstashLayout layout = new LogstashLayout();

    private ExecutorService executor;

    private ScheduledExecutorService cleanExecutor;

    private int appenderCount = 0;

    private Producer<String, String> producer;

    private long lastPrintOverloadTime = -1L;

    /* configurable params */

    private Map<String, Object> producerConfig = new HashMap<>();

    private int queueSize = DEFAULT_QUEUE_SIZE;

    private int discardingThreshold = UNDEFINED;

    private String topic;

    private boolean daemonThread = true;

    private boolean ignoreOverload = false;

    private int maximumFrequency = 1000;

    public LogstashKafkaAppender() {

        // setting default producer config
        producerConfig.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerConfig.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerConfig.put("acks", "0");
        producerConfig.put("retries", 0);
        producerConfig.put("batch.size", 16384 * 2);
        producerConfig.put("linger.ms", 1000);
        producerConfig.put("block.on.buffer.full", false);
    }

    public void run() {

        while (this.isStarted()) {
            try {
                ILoggingEvent event = this.blockingQueue.take();
                if (event != null) {
                    push(event);
                }
            } catch (Exception ie) {
                break;
            }
        }

        addInfo("Worker thread will flush remaining events before exiting. ");

        for (ILoggingEvent event : this.blockingQueue) {
            push(event);
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

            if (layout.isIncludeCallerData()) {
                event.getCallerData();
            }

            blockingQueue.put(event);

        } catch (InterruptedException ignored) {

        }
    }

    /**
     * push log to kafka
     */
    private void push(final ILoggingEvent event) {

        try {
            if (ignoreOverload || eventsPerSecond.incrementAndGet() <= maximumFrequency) {

                producer.send(new ProducerRecord<String, String>(topic, null, layout.doLayout(event)), new Callback() {
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        if (appenderCount > 0 && exception != null) {
                            addInfo("record log to kafka failed, Current frequency is " + eventsPerSecond.get()
                                    + " per second.", exception);
                            appendLoopOnAppenders(event);
                        }
                    }
                });
            } else {
                // 超负载后，采用其它appender记录日志
                if (appenderCount > 0) {
                    if (lastPrintOverloadTime < 0 || System.currentTimeMillis() - lastPrintOverloadTime > 1000) {
                        addInfo("append event overload, Current frequency is " + eventsPerSecond.get() + " per second");
                        lastPrintOverloadTime = System.currentTimeMillis();
                    }
                    this.appendLoopOnAppenders(event);
                }
            }
        } catch (Throwable e) {

            addInfo("record log to kafka failed. exMsg:" + e.getMessage());
            if (appenderCount > 0) {
                this.appendLoopOnAppenders(event);
            }
        }
    }

    private void appendLoopOnAppenders(ILoggingEvent event) {
        this.aai.appendLoopOnAppenders(event);
    }

    @Override
    public void start() {
        addInfo("Kafka producer config is: " + producerConfig);

        if (producerConfig.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG) == null) {
            addError("No \"" + ProducerConfig.BOOTSTRAP_SERVERS_CONFIG + "\" set for the appender named [\"" + name
                    + "\"].");
            return;
        }

        if (topic == null) {
            addError("No topic set for the appender named [\"" + name + "\"].");
            return;
        }

        super.start();

        blockingQueue = new ArrayBlockingQueue<>(queueSize);

        if (discardingThreshold == UNDEFINED)
            discardingThreshold = queueSize / 5;
        addInfo("Setting discardingThreshold to " + discardingThreshold);

        // 初始化kafka producer
        producer = new KafkaProducer<>(producerConfig);

        executor = Executors
                .newSingleThreadExecutor(new NamedThreadFactory(getName() + "-KafkaAppender", daemonThread));
        executor.submit(this);
        cleanExecutor = Executors.newSingleThreadScheduledExecutor(
                new NamedThreadFactory(getName() + "-frequency-clean-thread", daemonThread));
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
    }

    private boolean isDiscardable(ILoggingEvent event) {
        Level level = event.getLevel();
        return level.toInt() <= Level.INFO_INT;
    }

    private boolean isQueueBelowDiscardingThreshold() {
        return (blockingQueue.remainingCapacity() < discardingThreshold);
    }

    public LogstashLayout getLayout() {
        return layout;
    }

    public void setLayout(LogstashLayout layout) {
        this.layout = layout;
    }

    public void addAppender(Appender<ILoggingEvent> newAppender) {
        appenderCount++;
        addInfo("Attaching appender named [" + newAppender.getName() + "] to LogstashKafkaAppender.");
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

    public Map<String, Object> getProducerConfig() {
        return producerConfig;
    }

    public void addProducerConfig(String keyValue) {
        String[] split = keyValue.split("=", 2);
        if (split.length == 2) {
            this.producerConfig.put(split[0], split[1]);
        } else {
            addWarn("Illegal param for producerConfig: [" + keyValue + "] ");
        }
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getTopic() {
        return topic;
    }

    public int getQueueSize() {
        return queueSize;
    }

    public void setQueueSize(int queueSize) {
        this.queueSize = queueSize;
    }

    public int getDiscardingThreshold() {
        return discardingThreshold;
    }

    public void setDiscardingThreshold(int discardingThreshold) {
        this.discardingThreshold = discardingThreshold;
    }

    public boolean isDaemonThread() {
        return daemonThread;
    }

    public void setDaemonThread(boolean daemonThread) {
        this.daemonThread = daemonThread;
    }

    public boolean isIgnoreOverload() {
        return ignoreOverload;
    }

    public void setIgnoreOverload(boolean ignoreOverload) {
        this.ignoreOverload = ignoreOverload;
    }

    public int getMaximumFrequency() {
        return maximumFrequency;
    }

    public void setMaximumFrequency(int maximumFrequency) {
        this.maximumFrequency = maximumFrequency;
    }
}
