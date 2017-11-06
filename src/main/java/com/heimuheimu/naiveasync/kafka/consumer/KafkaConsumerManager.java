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

import com.heimuheimu.naiveasync.BeanStatusEnum;
import com.heimuheimu.naiveasync.consumer.AsyncMessageConsumer;
import com.heimuheimu.naiveasync.kafka.KafkaUtil;
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
 * 基于 Kafka 框架实现的消费者管理器，更多 Kafka 信息请参考：<a href="http://kafka.apache.org/documentation/">http://kafka.apache.org/documentation/</a>。
 *
 * <p>
 *     {@code KafkaConsumerManager} 实例应调用 {@link #init()} 方法，初始化成功后才可使用。
 *     当 {@code KafkaConsumerManager} 不再使用时，应调用 {@link #close()} 方法进行资源释放。
 * </p>
 *
 * <p><strong>说明：</strong>{@code KafkaConsumerManager} 类是线程安全的，可在多个线程中使用同一个实例。</p>
 *
 * @author heimuheimu
 * @see AsyncMessageConsumer
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
     * 同一条消息最大连续消费失败次数
     */
    private final int maxConsumeRetryTimes;

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
     * @param maxConsumeRetryTimes 同一条消息最大连续消费失败次数，不允许小于 0
     * @param kafkaConsumerListener Kafka 消费者事件监听器，允许为 {@code null}
     * @throws IllegalArgumentException 如果消费者列表为 {@code null} 或空列表，将抛出此异常
     * @throws IllegalArgumentException 如果 Kafka 消费者配置信息为 {@code null}，将抛出此异常
     * @throws IllegalArgumentException 如果同一条消息最大连续消费失败次数小于 0，将抛出此异常
     */
    public KafkaConsumerManager(List<AsyncMessageConsumer<?>> consumers, KafkaConsumerConfig kafkaConsumerConfig,
            int maxConsumeRetryTimes, KafkaConsumerListener kafkaConsumerListener) throws IllegalArgumentException {
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
        this.maxConsumeRetryTimes = maxConsumeRetryTimes;
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
                ", maxConsumeRetryTimes=" + maxConsumeRetryTimes +
                ", listener=" + listener +
                ", state=" + state +
                '}';
    }

    /**
     * Kafka 消费者线程
     */
    private class KafkaConsumeThread extends Thread {

        private final LinkedList<Map<TopicPartition, OffsetAndMetadata>> failedCommitOffsetQueue = new LinkedList<>();

        private final Map<TopicPartition, Long> continuesConsumeFailedCountMap = new HashMap<>();

        private final KafkaConsumer<byte[], byte[]> consumer;

        private final String topic;

        private final AsyncMessageConsumer asyncMessageConsumer;

        private volatile boolean stopFlag = false;

        private boolean isPollFailed = false;

        private  KafkaConsumeThread(AsyncMessageConsumer asyncMessageConsumer) {
            this.consumer = new KafkaConsumer<>(config.toConfigMap());
            this.topic = KafkaUtil.getTopicName(asyncMessageConsumer.getMessageClass());
            this.asyncMessageConsumer = asyncMessageConsumer;

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

        @Override
        @SuppressWarnings("unchecked")
        public void run() {
            try {
                CONSUMER_INFO_LOGGER.info("Kafka consumer thread has been started. Thread: `{}`. Topic: `{}`. Consumer: `{}`. Assigned partitions: `{}`. KafkaConsumerConfig: `{}`.",
                        getName(), topic, asyncMessageConsumer, consumer.assignment(), config);
                ConsumerRecords<byte[], byte[]> records;
                while (!stopFlag) {
                    if (!failedCommitOffsetQueue.isEmpty()) {
                        Map<TopicPartition, OffsetAndMetadata> kafkaOffset;
                        while ((kafkaOffset = failedCommitOffsetQueue.poll()) != null && !stopFlag) {
                            commitSync(kafkaOffset, true);
                        }
                    }
                    records = null;
                    try {
                        if (!stopFlag) {
                            records = consumer.poll(Long.MAX_VALUE);
                            if (isPollFailed) {
                                isPollFailed = false;
                                listener.onPollRecovered(config.getBootstrapServers());
                            }
                        }
                    } catch (InterruptException ignored) {
                        //do nothing
                    } catch (Exception e) {
                        LOGGER.error("Poll message failed: `" + e.getMessage() + "`. Assigned partitions: `" + consumer.assignment() +
                                "`. Thread: `" + getName() + "`. KafkaConsumerConfig: `" + config + "`.", e);
                        monitor.onErrorExecution();
                        isPollFailed = true;
                        listener.onPollFailed(config.getBootstrapServers());
                        try {
                            Thread.sleep(5000);
                        } catch (InterruptedException ignored) {}
                    }
                    if (records != null) {
                        for (TopicPartition partition : records.partitions()) { //按分区进行遍历
                            if (asyncMessageConsumer != null) {
                                List<ConsumerRecord<byte[], byte[]>> partitionRecords = records.records(partition);
                                Long lastOffset = null;
                                for (ConsumerRecord<byte[], byte[]> record : partitionRecords) {
                                    Object message = null;
                                    try {
                                        message = transcoder.decode(record.value());
                                        asyncMessageConsumer.consume(message);
                                        lastOffset = record.offset();
                                        continuesConsumeFailedCountMap.remove(partition);
                                        monitor.onSuccessConsumed(topic, 1);
                                    } catch (Exception e) {
                                        LOGGER.error("Consume message failed: `" + e.getMessage() + "`. Thread: `"
                                            + getName() + "`. TopicPartition: `" + partition + "`. KafkaConsumerConfig: `" + config
                                            + "`. Message: `" + message + "`.", e);
                                        monitor.onErrorExecution();
                                        listener.onConsumeFailed(partition, message, config.getBootstrapServers());
                                        addConsumeFailedCount(partition);
                                        break; //消息消费发生异常，该分区已获取的其它消息不再进行消费
                                    }
                                }
                                if (lastOffset != null) {
                                    Map<TopicPartition, OffsetAndMetadata> kafkaOffset = Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1));
                                    commitSync(kafkaOffset, false);
                                }
                            } else { //should not happen
                                LOGGER.error("There is no consumer for TopicPartition `{}`. Thread: `{}`. KafkaConsumerConfig: `{}`.",
                                        partition, getName(), config);
                                monitor.onErrorExecution();
                            }
                        }
                    }
                }
            } catch (Exception e) {//should not happen
                LOGGER.error("KafkaConsumeThread need to be closed due to unexpected error. Thread: `" + getName()
                        + "`. KafkaConsumerConfig: `" + config + "`.", e);
            } finally {
                close();
                try {
                    consumer.close();
                } catch (Exception e) {
                    LOGGER.error("KafkaConsumer closed failed. Thread: `" + getName() + "`. Topic: `" + topic + "`.", e);
                }
                CONSUMER_INFO_LOGGER.info("Kafka consumer thread has been stopped. Thread: `{}`. Topic: `{}`. KafkaConsumerConfig: `{}`.", getName(), topic, config);
            }
        }

        private void commitSync(Map<TopicPartition, OffsetAndMetadata> offset, boolean isRecovered) {
            TopicPartition partition = null;
            try {
                partition = offset.keySet().iterator().next(); //must exist
                consumer.commitSync(offset);

                if (isRecovered) {
                    listener.onCommitSyncRecovered(partition, config.getBootstrapServers());
                }
            } catch (Exception e) {
                LOGGER.error("Commit sync failed. Offset: `" + offset + "`. Thread: `" + getName() + "`. KafkaConsumerConfig: `"
                        + config + "`.", e);
                monitor.onErrorExecution();
                failedCommitOffsetQueue.offer(offset);
                listener.onCommitSyncFailed(partition, config.getBootstrapServers());
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException ignored) {}
            }
        }

        private void addConsumeFailedCount(TopicPartition partition) {
            Long failedCount = continuesConsumeFailedCountMap.get(partition);
            failedCount = (failedCount != null) ? (failedCount + 1) : 1;
            continuesConsumeFailedCountMap.put(partition, failedCount);
            if (failedCount > maxConsumeRetryTimes) {
                try {
                    consumer.pause(Collections.singletonList(partition));
                    listener.onPartitionPaused(partition, config.getBootstrapServers());
                } catch (Exception e) {
                    LOGGER.error("Pause partition failed. TopicPartition: `" + partition + "`. Thread: `" + getName()
                            + "`. KafkaConsumerConfig: `" + config + "`.", e);
                }
            }
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
        public void onPollFailed(String bootstrapServers) {
            if (listener != null) {
                try {
                    listener.onPollFailed(bootstrapServers);
                } catch (Exception e) {
                    LOGGER.error("Call KafkaConsumerListener#onPollFailed() failed. KafkaConsumerConfig: `" + config + "`.", e);
                }
            }
        }

        @Override
        public void onPollRecovered(String bootstrapServers) {
            if (listener != null) {
                try {
                    listener.onPollRecovered(bootstrapServers);
                } catch (Exception e) {
                    LOGGER.error("Call KafkaConsumerListener#onPollRecovered() failed. KafkaConsumerConfig: `" + config + "`.", e);
                }
            }
        }

        @Override
        public void onConsumeFailed(TopicPartition partition, Object message, String bootstrapServers) {
            if (listener != null) {
                try {
                    listener.onConsumeFailed(partition, message, bootstrapServers);
                } catch (Exception e) {
                    LOGGER.error("Call KafkaConsumerListener#onConsumeFailed() failed. TopicPartition: `" + partition + "`. Message: `"
                            + message + "`. KafkaConsumerConfig: `" + config + "`.", e);
                }
            }
        }

        @Override
        public void onCommitSyncFailed(TopicPartition partition, String bootstrapServers) {
            if (listener != null) {
                try {
                    listener.onCommitSyncFailed(partition, bootstrapServers);
                } catch (Exception e) {
                    LOGGER.error("Call KafkaConsumerListener#onCommitSyncFailed() failed. TopicPartition: `" + partition
                            + "`. KafkaConsumerConfig: `" + config + "`.", e);
                }
            }
        }

        @Override
        public void onCommitSyncRecovered(TopicPartition partition, String bootstrapServers) {
            if (listener != null) {
                try {
                    listener.onCommitSyncRecovered(partition, bootstrapServers);
                } catch (Exception e) {
                    LOGGER.error("Call KafkaConsumerListener#onCommitSyncRecovered() failed. TopicPartition: `" + partition
                            + "`. KafkaConsumerConfig: `" + config + "`.", e);
                }
            }
        }

        @Override
        public void onPartitionPaused(TopicPartition partition, String bootstrapServers) {
            if (listener != null) {
                try {
                    listener.onPartitionPaused(partition, bootstrapServers);
                } catch (Exception e) {
                    LOGGER.error("Call KafkaConsumerListener#onPartitionPaused() failed. TopicPartition: `" + partition
                            + "`. KafkaConsumerConfig: `" + config + "`.", e);
                }
            }
        }
    }
}
