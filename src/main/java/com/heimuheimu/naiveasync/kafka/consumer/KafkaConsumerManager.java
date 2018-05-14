/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2017 heimuheimu
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package com.heimuheimu.naiveasync.kafka.consumer;

import com.heimuheimu.naiveasync.constant.BeanStatusEnum;
import com.heimuheimu.naiveasync.consumer.AsyncMessageConsumer;
import com.heimuheimu.naiveasync.kafka.util.KafkaUtil;
import com.heimuheimu.naiveasync.monitor.consumer.AsyncMessageConsumerMonitor;
import com.heimuheimu.naiveasync.monitor.consumer.AsyncMessageConsumerMonitorFactory;
import com.heimuheimu.naiveasync.transcoder.MessageTranscoder;
import com.heimuheimu.naiveasync.transcoder.SimpleMessageTranscoder;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.*;

/**
 * 基于 Kafka 框架实现的消费者管理器。
 *
 * <h3>Kafka 消费者 Log4j 配置</h3>
 * <blockquote>
 * <pre>
 * log4j.logger.NAIVE_ASYNC_CONSUMER_INFO_LOG=INFO, NAIVE_ASYNC_CONSUMER_INFO_LOG
 * log4j.additivity.NAIVE_ASYNC_CONSUMER_INFO_LOG=false
 * log4j.appender.NAIVE_ASYNC_CONSUMER_INFO_LOG=org.apache.log4j.DailyRollingFileAppender
 * log4j.appender.NAIVE_ASYNC_CONSUMER_INFO_LOG.file=${log.output.directory}/naiveasync/consumer_info.log
 * log4j.appender.NAIVE_ASYNC_CONSUMER_INFO_LOG.encoding=UTF-8
 * log4j.appender.NAIVE_ASYNC_CONSUMER_INFO_LOG.DatePattern=_yyyy-MM-dd
 * log4j.appender.NAIVE_ASYNC_CONSUMER_INFO_LOG.layout=org.apache.log4j.PatternLayout
 * log4j.appender.NAIVE_ASYNC_CONSUMER_INFO_LOG.layout.ConversionPattern=%d{ISO8601} %-5p : %m%n
 * </pre>
 * </blockquote>
 *
 * <p><strong>说明：</strong>{@code KafkaConsumerManager} 类是线程安全的，可在多个线程中使用同一个实例。</p>
 *
 * <p>更多 Kafka 信息请参考：<a href="http://kafka.apache.org/documentation/">http://kafka.apache.org/documentation/</a>。</p>
 *
 * @see AsyncMessageConsumer
 * @author heimuheimu
 */
public class KafkaConsumerManager implements Closeable {

    private final static Logger LOGGER = LoggerFactory.getLogger(KafkaConsumerManager.class);

    private final static Logger CONSUMER_INFO_LOGGER = LoggerFactory.getLogger("NAIVE_ASYNC_CONSUMER_INFO_LOG");

    /**
     * Kafka 异步消息消费者 {@code Map}，Key 为消息所在 Topic 名称，Value 为消费者
     */
    private final Map<String, AsyncMessageConsumer<?>> consumerMap;

    /**
     * Kafka 消费者配置信息
     */
    private final KafkaConsumerConfig config;

    /**
     * Kafka 消费者事件监听器
     */
    private final KafkaConsumerListener listener;

    /**
     * 消息与字节数组转换器
     */
    private final MessageTranscoder transcoder;

    /**
     * 异步消息消费者信息监控器
     */
    private final AsyncMessageConsumerMonitor monitor;

    /**
     * 管理者管理的 Kafka 消费者线程
     */
    private final List<KafkaConsumeThread> consumeThreadList = new ArrayList<>();

    /**
     * {@code KafkaConsumerManager} 实例状态
     */
    private BeanStatusEnum state = BeanStatusEnum.UNINITIALIZED;

    /**
     * 构造一个 Kafka 异步消息消费者管理器。
     *
     * @param consumers 消费者列表，不允许为 {@code null} 或空列表
     * @param kafkaConsumerConfig Kafka 消费者配置信息，不允许为 {@code null}
     * @param kafkaConsumerListener Kafka 消费者事件监听器，允许为 {@code null}
     * @throws IllegalArgumentException 如果消费者列表为 {@code null} 或空列表，将抛出此异常
     * @throws IllegalArgumentException 如果 Kafka 消费者配置信息为 {@code null}，将抛出此异常
     * @throws IllegalArgumentException 如果同一条消息最大连续消费失败次数小于 0，将抛出此异常
     */
    public KafkaConsumerManager(List<AsyncMessageConsumer<?>> consumers, KafkaConsumerConfig kafkaConsumerConfig,
            KafkaConsumerListener kafkaConsumerListener) throws IllegalArgumentException {
        if (consumers == null || consumers.isEmpty()) {
            LOGGER.error("Create KafkaConsumerManager failed: `consumers could not be null or empty`. Consumers: `"
                    + consumers + "`. KafkaConsumerConfig: `" + kafkaConsumerConfig + "`. KafkaConsumerListener: `"
                    + kafkaConsumerListener + "`.");
            throw new IllegalArgumentException("Create KafkaConsumerManager failed: `consumers could not be null or empty`. Consumers: `"
                    + consumers + "`. KafkaConsumerConfig: `" + kafkaConsumerConfig + "`. KafkaConsumerListener: `"
                    + kafkaConsumerListener + "`.");
        }
        if (kafkaConsumerConfig == null) {
            LOGGER.error("Create KafkaConsumerManager failed: `kafkaConsumerConfig could not be null or empty`. Consumers: `"
                    + consumers + "`. KafkaConsumerConfig: `null`. KafkaConsumerListener: `"
                    + kafkaConsumerListener + "`.");
            throw new IllegalArgumentException("Create KafkaConsumerManager failed: `kafkaConsumerConfig could not be null or empty`. Consumers: `"
                    + consumers + "`. KafkaConsumerConfig: `null`. KafkaConsumerListener: `"
                    + kafkaConsumerListener + "`.");
        }
        this.consumerMap = new HashMap<>();
        for (AsyncMessageConsumer<?> consumer : consumers) {
            String topicName = KafkaUtil.getTopicName(consumer.getMessageClass());
            AsyncMessageConsumer<?> existedConsumer = this.consumerMap.get(topicName);
            if (existedConsumer != null && existedConsumer != consumer) {
                LOGGER.error("Consumer `{}` is existed. It will be overridden. Previous consumer: `{}`. New Consumer: `{}`. KafkaConsumerConfig: `{}`.",
                        topicName, existedConsumer, consumer, kafkaConsumerConfig);
            }
            this.consumerMap.put(topicName, consumer);
        }
        this.config = kafkaConsumerConfig;
        this.listener = new KafkaConsumerListenerWrapper(kafkaConsumerListener);
        this.transcoder = new SimpleMessageTranscoder();
        this.monitor = AsyncMessageConsumerMonitorFactory.get();
    }

    /**
     * 执行 {@code KafkaConsumerManager} 初始化操作，初始化完成后，重复执行该方法不会产生任何效果。
     *
     * @throws IllegalStateException 如果初始化过程中出现错误，将抛出此异常
     */
    public synchronized void init() throws IllegalStateException {
        long startTime = System.currentTimeMillis();
        if (state == BeanStatusEnum.UNINITIALIZED) {
            state = BeanStatusEnum.NORMAL;
            try {
                for (String topic : consumerMap.keySet()) {
                    AsyncMessageConsumer<?> consumer = consumerMap.get(topic);
                    int threadPoolSize = 1;
                    if (consumer instanceof KafkaAsyncMessageConsumer) {
                        threadPoolSize = ((KafkaAsyncMessageConsumer) consumer).getPoolSize();
                    }
                    for (int i = 0; i < threadPoolSize; i++) {
                        KafkaConsumeThread consumeThread = new KafkaConsumeThread(consumer);
                        consumeThread.setName("naiveasync-kafka-consumer-" + i + "[" + topic + "]");
                        consumeThread.start();
                        consumeThreadList.add(consumeThread);
                    }
                }
                CONSUMER_INFO_LOGGER.info("KafkaConsumerManager has been initialized. Cost: `{} ms`. KafkaConsumerConfig: `{}`. PoolSize: `{}`. Topics: `{}`. Listener: `{}`.",
                        (System.currentTimeMillis() - startTime), config, consumeThreadList.size(), consumerMap.keySet(), listener);
            } catch (Exception e) {
                LOGGER.error("KafkaConsumerManager initialize failed. KafkaConsumerConfig: `" + config + "`. PoolSize: `" + consumeThreadList.size()
                                + "`. Topics: `" + consumerMap.keySet() + "`. Listener: `" + listener + "`.", e);
                close();
                throw new IllegalStateException("KafkaConsumerManager initialize failed. KafkaConsumerConfig: `" + config + "`. PoolSize: `" + consumeThreadList.size()
                        + "`. Topics: `" + consumerMap.keySet() + "`. Listener: `" + listener + "`.", e);
            }
        }
    }

    /**
     * 执行  {@code KafkaConsumerManager} 关闭操作，在关闭完成后，重复执行该方法不会产生任何效果。
     *
     * <p><strong>注意：</strong>该方法不会抛出任何异常。
     */
    @Override
    public synchronized void close() {
        long startTime = System.currentTimeMillis();
        if (state != BeanStatusEnum.CLOSED) {
            state = BeanStatusEnum.CLOSED;
            for (KafkaConsumeThread consumeThread : consumeThreadList) {
                try {
                    consumeThread.close();
                } catch (Exception e) {
                    LOGGER.error("KafkaConsumeThread close failed. ThreadName: `" + consumeThread.getName() + "`. KafkaConsumerConfig: `"
                            + config + "`.", e);
                }
            }
            CONSUMER_INFO_LOGGER.info("KafkaConsumerManager has been stopped. Cost: `{} ms`. KafkaConsumerConfig: `{}`. PoolSize: `{}`. Topics: `{}`. Listener: `{}`.",
                    (System.currentTimeMillis() - startTime), config, consumeThreadList.size(), consumerMap.keySet(), listener);
        }
    }

    @Override
    public String toString() {
        return "KafkaConsumerManager{" +
                "consumerMap=" + consumerMap +
                ", config=" + config +
                ", listener=" + listener +
                ", state=" + state +
                '}';
    }

    /**
     * Kafka 消费者线程。
     */
    private class KafkaConsumeThread extends Thread {

        private final String topic;

        private final AsyncMessageConsumer asyncMessageConsumer;

        private volatile boolean stopFlag = false;

        private KafkaConsumer<byte[], byte[]> consumer;

        private int sleepSeconds = 2;

        private boolean hasError = false;

        private KafkaConsumeThread(AsyncMessageConsumer asyncMessageConsumer) {
            this.topic = KafkaUtil.getTopicName(asyncMessageConsumer.getMessageClass());
            this.asyncMessageConsumer = asyncMessageConsumer;
            createConsumer();
        }

        @Override
        @SuppressWarnings("unchecked")
        public void run() {
            try {
                CONSUMER_INFO_LOGGER.info("Kafka consumer thread has been started. Thread: `{}`. Topic: `{}`. Consumer: `{}`. Assigned partitions: `{}`. KafkaConsumerConfig: `{}`.",
                        getName(), topic, asyncMessageConsumer, consumer.assignment(), config);
                consumeWhile: while (!stopFlag) {
                    if (consumer == null) {
                        try {
                            createConsumer();
                        } catch (Exception e) {
                            LOGGER.error("Create kafka consumer failed: `" + e.getMessage() + "`. Thread: `" + getName()
                                    + "`. Topic: `" + topic + "`.", e);
                            monitor.onErrorExecution();
                            onError("create consumer failed", true);
                            continue;
                        }
                    }
                    ConsumerRecords<byte[], byte[]> records = null;
                    try {
                        if (!stopFlag) {
                            records = consumer.poll(Long.MAX_VALUE);
                        }
                    } catch (InterruptException ignored) {
                        //do nothing
                    } catch (Exception e) {
                        LOGGER.error("Poll message failed: `" + e.getMessage() + "`. Assigned partitions: `" + consumer.assignment() +
                                "`. Thread: `" + getName() + "`. KafkaConsumerConfig: `" + config + "`.", e);
                        monitor.onErrorExecution();
                        onError("poll failed", true);
                        continue;
                    }
                    if (records != null) {
                        for (TopicPartition partition : records.partitions()) { //按分区进行遍历
                            List<Object> messageList = new ArrayList<>();
                            List<Long> offsetList = new ArrayList<>();
                            List<ConsumerRecord<byte[], byte[]>> partitionRecords = records.records(partition);
                            Long lastOffset = null;
                            for (ConsumerRecord<byte[], byte[]> record : partitionRecords) {
                                monitor.onPolled(topic, System.currentTimeMillis() - record.timestamp());
                                Object message = null;
                                try {
                                    message = transcoder.decode(record.value());
                                    messageList.add(message);
                                    offsetList.add(record.offset());
                                } catch (Exception e) {
                                    LOGGER.error("Decode message failed: `" + e.getMessage() + "`. Thread: `"
                                        + getName() + "`. TopicPartition: `" + partition + "`. KafkaConsumerConfig: `" + config
                                        + "`. Message: `" + message + "`.", e);
                                    monitor.onErrorExecution();
                                    onError("decode message failed", false);
                                    continue consumeWhile;
                                }
                            }

                            if (!messageList.isEmpty()) {
                                if (asyncMessageConsumer.isBatchMode()) {
                                    try {
                                        asyncMessageConsumer.consume(messageList);
                                    } catch (Exception e) {
                                        LOGGER.error("Consume message failed: `" + e.getMessage() + "`. Thread: `"
                                                + getName() + "`. TopicPartition: `" + partition + "`. KafkaConsumerConfig: `" + config
                                                + "`. MessageList: `" + messageList + "`.", e);
                                        monitor.onErrorExecution();
                                        onError("consume message failed", false);
                                        continue consumeWhile;
                                    }
                                    try {
                                        commitSync(partition, offsetList.get(offsetList.size() - 1));
                                    } catch (Exception e) {
                                        LOGGER.error("Commit sync failed: `" + e.getMessage() + "`. Thread: `"
                                                + getName() + "`. TopicPartition: `" + partition + "`. KafkaConsumerConfig: `"
                                                + config + "`.", e);
                                        onError("commit sync failed", true);
                                        continue consumeWhile;
                                    }
                                    monitor.onSuccessConsumed(topic, messageList.size());
                                } else {
                                    for (int i = 0; i < messageList.size(); i++) {
                                        Object message = messageList.get(i);
                                        long offset = offsetList.get(i);
                                        try {
                                            asyncMessageConsumer.consume(message);
                                        } catch (Exception e) {
                                            LOGGER.error("Consume message failed: `" + e.getMessage() + "`. Thread: `"
                                                    + getName() + "`. TopicPartition: `" + partition + "`. KafkaConsumerConfig: `" + config
                                                    + "`. Message: `" + message + "`.", e);
                                            monitor.onErrorExecution();
                                            onError("consume message failed", false);
                                            continue consumeWhile;
                                        }
                                        try {
                                            commitSync(partition, offset);
                                        } catch (Exception e) {
                                            LOGGER.error("Commit sync failed: `" + e.getMessage() + "`. Thread: `"
                                                    + getName() + "`. TopicPartition: `" + partition + "`. KafkaConsumerConfig: `"
                                                    + config + "`.", e);
                                            onError("commit sync failed", true);
                                            continue consumeWhile;
                                        }
                                        monitor.onSuccessConsumed(topic, 1);
                                    }
                                }
                            }
                        }
                    }
                    onSuccess();
                }
            } catch (Exception e) {//should not happen
                LOGGER.error("KafkaConsumeThread need to be closed due to unexpected error. Thread: `" + getName()
                        + "`. KafkaConsumerConfig: `" + config + "`.", e);
            } finally {
                close();
                try {
                    consumer.close();
                } catch (InterruptException ignored) {
                    //ignored exception
                } catch (Exception e) {
                    LOGGER.error("KafkaConsumer closed failed. Thread: `" + getName() + "`. Topic: `" + topic + "`.", e);
                }
                CONSUMER_INFO_LOGGER.info("Kafka consumer thread has been stopped. Thread: `{}`. Topic: `{}`. KafkaConsumerConfig: `{}`.", getName(), topic, config);
            }
        }

        /**
         * 创建 {@code KafkaConsumer} 实例。
         */
        private void createConsumer() {
            this.consumer = new KafkaConsumer<>(config.toConfigMap());
            consumer.subscribe(Collections.singletonList(topic), new ConsumerRebalanceListener() {

                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                    CONSUMER_INFO_LOGGER.info("[Rebalance] Revoked partitions: `{}`. Thread: `{}`. Topic: `{}`. KafkaConsumerConfig: `{}`.",
                            partitions, getName(), topic, config);
                }

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    CONSUMER_INFO_LOGGER.info("[Rebalance] Assigned partitions: `{}`. Thread: `{}`. Topic: `{}`. KafkaConsumerConfig: `{}`.",
                            partitions, getName(), topic, config);
                }
            });
        }

        /**
         * 当 {@code KafkaConsumer} 操作出现异常时，调用此方法。
         */
        private void onError(String errorMessage, boolean needCloseConsumer) {
            if (needCloseConsumer && this.consumer != null) {
                try {
                    this.consumer.close();
                } catch (Exception e) {
                    LOGGER.error("KafkaConsumer closed failed. Thread: `" + getName() + "`. Topic: `" + topic + "`.", e);
                }
                this.consumer = null;
            }
            hasError = true;
            try {
                Thread.sleep(sleepSeconds * 1000);
            } catch (InterruptedException ignored) {}
            sleepSeconds *= 2;
            listener.onError(errorMessage, config.getGroupId(), config.getBootstrapServers());
        }

        /**
         * 当 {@code KafkaConsumer} 操作成功时，调用此方法。
         */
        private void onSuccess() {
            sleepSeconds = 2;
            if (hasError) {
                listener.onRecover(config.getGroupId(), config.getBootstrapServers());
                hasError = false;
            }
        }

        private void commitSync(TopicPartition partition, long lastOffset) {
            Map<TopicPartition, OffsetAndMetadata> kafkaOffset = Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1));
            consumer.commitSync(kafkaOffset);
        }

        private synchronized void close() {
            if (!stopFlag) {
                try {
                    stopFlag = true;
                    interrupt();
                } catch (Exception e) {
                    LOGGER.error("Consumer closed failed. Thread: `" + getName() + "`. KafkaConsumerConfig: `" + config + "`.", e);
                }
            }
        }
    }

    private class KafkaConsumerListenerWrapper implements KafkaConsumerListener {

        private final KafkaConsumerListener listener;

        public KafkaConsumerListenerWrapper(KafkaConsumerListener listener) {
            this.listener = listener;
        }

        @Override
        public void onError(String errorMessage, String groupId, String bootstrapServers) {
            if (listener != null) {
                try {
                    listener.onError(errorMessage, groupId, bootstrapServers);
                } catch (Exception e) {
                    LOGGER.error("Call KafkaConsumerListener#onError() failed. ErrorMessage: `" + errorMessage
                            + "`. KafkaConsumerConfig: `" + config + "`.", e);
                }
            }
        }

        @Override
        public void onRecover(String groupId, String bootstrapServers) {
            if (listener != null) {
                try {
                    listener.onRecover(groupId, bootstrapServers);
                } catch (Exception e) {
                    LOGGER.error("Call KafkaConsumerListener#onRecover() failed. KafkaConsumerConfig: `" + config + "`.", e);
                }
            }
        }
    }
}
