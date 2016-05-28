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
package net.logstash.logback.util;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * NamedThreadFactory
 * Created by aqlu on 14-10-15.
 */
@SuppressWarnings("WeakerAccess")
public class NamedThreadFactory implements ThreadFactory {

    private final String prefix;

    private final ThreadFactory threadFactory;

    private final boolean daemonThread;

    private final AtomicInteger counter = new AtomicInteger();

    public NamedThreadFactory(final String prefix, final boolean daemonThread) {
        this(prefix, daemonThread, Executors.defaultThreadFactory());
    }

    public NamedThreadFactory(final String prefix, final boolean daemonThread, final ThreadFactory threadFactory) {
        this.prefix = prefix;
        this.threadFactory = threadFactory;
        this.daemonThread = daemonThread;
    }

    public Thread newThread(Runnable r) {
        Thread t = this.threadFactory.newThread(r);
        t.setDaemon(this.daemonThread);
        t.setName(this.prefix + "-Thread-" + this.counter.incrementAndGet());
        return t;
    }

}
