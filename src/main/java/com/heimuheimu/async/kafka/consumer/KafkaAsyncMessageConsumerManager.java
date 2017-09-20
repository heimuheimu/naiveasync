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

package com.heimuheimu.async.kafka.consumer;

import com.heimuheimu.async.BeanStatusEnum;
import com.heimuheimu.async.consumer.AsyncMessageConsumer;
import com.heimuheimu.async.kafka.KafkaUtil;
import com.heimuheimu.async.monitor.consumer.AsyncMessageConsumerMonitor;
import com.heimuheimu.async.monitor.consumer.AsyncMessageConsumerMonitorFactory;
import com.heimuheimu.async.transcoder.MessageTranscoder;
import com.heimuheimu.async.transcoder.SimpleMessageTranscoder;
import com.heimuheimu.async.transcoder.TranscoderException;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.*;

/**
 * 基于 kafka 框架实现的异步消息消费者管理器，更多 kafka 信息请参考：<a href="http://kafka.apache.org/documentation/">http://kafka.apache.org/documentation/</a>
 *
 * @author heimuheimu
 */
public class KafkaAsyncMessageConsumerManager implements Closeable {

    private final static Logger CONSUMER_INFO_LOG = LoggerFactory.getLogger("NAIVE_ASYNC_CONSUMER_INFO_LOG");

    private final static Logger CONSUMER_ERROR_LOG = LoggerFactory.getLogger("NAIVE_ASYNC_CONSUMER_ERROR_LOG");

    private final Map<String, AsyncMessageConsumer<?>> consumerMap;

    private final List<String> topicList;

    private final KafkaConsumerConfig config;

    private final KafkaConsumerListener listener;

    private final int poolSize;

    private final MessageTranscoder transcoder;

    private final AsyncMessageConsumerMonitor monitor;

    private final List<KafkaConsumeThread> consumeThreadList = new ArrayList<>();

    private BeanStatusEnum state = BeanStatusEnum.UNINITIALIZED;

    public KafkaAsyncMessageConsumerManager(List<AsyncMessageConsumer<?>> consumers, KafkaConsumerConfig config,
                                            KafkaConsumerListener listener, int poolSize) throws IllegalArgumentException {
        if (consumers == null || consumers.isEmpty()) {
            throw new IllegalArgumentException("Consumers could not be null or empty. Consumers: `" + consumers + "`. Config: `"
                + config + "`. Listener: `" + listener + "`. Pool size: `" + poolSize + "`.");
        }
        this.consumerMap = new HashMap<>();
        this.topicList = new ArrayList<>();
        for (AsyncMessageConsumer<?> consumer : consumers) {
            String topicName = KafkaUtil.getTopicName(consumer.getMessageClass());
            this.topicList.add(topicName);
            this.consumerMap.put(topicName, consumer);
        }

        this.config = config;
        this.listener = new KafkaConsumerListenerWrapper(listener);
        this.poolSize = poolSize;
        this.transcoder = new SimpleMessageTranscoder();
        this.monitor = AsyncMessageConsumerMonitorFactory.get();
    }

    public synchronized void init() {
        long startTime = System.currentTimeMillis();
        if (state == BeanStatusEnum.UNINITIALIZED) {
            for (int i = 0; i < poolSize; i++) {
                KafkaConsumeThread consumeThread = new KafkaConsumeThread();
                consumeThread.setName("kafka-consumer-" + i);
                consumeThread.start();
                consumeThreadList.add(consumeThread);
            }
            state = BeanStatusEnum.NORMAL;
            CONSUMER_INFO_LOG.info("KafkaAsyncMessageConsumerManager has been initialized. Cost: `{} ms`. Config: `{}`."
                    + " Pool size: `{}`. Topics: `{}`. Listener: `{}`.", (System.currentTimeMillis() - startTime),
                    config, poolSize, topicList, listener);
        }
    }

    @Override
    public synchronized void close() {
        long startTime = System.currentTimeMillis();
        if (state != BeanStatusEnum.CLOSED) {
            state = BeanStatusEnum.CLOSED;
            for (KafkaConsumeThread consumeThread : consumeThreadList) {
                consumeThread.close();
            }
            CONSUMER_INFO_LOG.info("KafkaAsyncMessageConsumerManager has been stopped. Cost: `{} ms`. Config: `{}`."
                    + " Pool size: `{}`. Topics: `{}`. Listener: `{}`.", (System.currentTimeMillis() - startTime),
                    config, poolSize, topicList, listener);
        }
    }

    private class KafkaConsumeThread extends Thread {

        private final KafkaConsumer<byte[], byte[]> consumer;

        private volatile boolean stopFlag = false;

        private boolean isPollFailed = false;

        private KafkaConsumeThread() {
            consumer = new KafkaConsumer<>(config.toConfigMap());
            consumer.subscribe(topicList, new ConsumerRebalanceListener() {

                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                    CONSUMER_INFO_LOG.info("[Rebalance] Revoked partitions: `{}`. Thread: `{}`. Config: `{}`.",
                            partitions, getName(), config);
                }

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    CONSUMER_INFO_LOG.info("[Rebalance] Assigned partitions: `{}`. Thread: `{}`. Config: `{}`.",
                            partitions, getName(), config);
                }
            });
        }

        @Override
        @SuppressWarnings("unchecked")
        public void run() {
            try {
                CONSUMER_INFO_LOG.info("Kafka consumer thread has been started. Assigned partitions: `{}`. Thread: `{}`. Config: `{}`.",
                        consumer.assignment(), getName(), config);
                ConsumerRecords<byte[], byte[]> records;
                Queue<Map<TopicPartition, OffsetAndMetadata>> kafkaOffsetQueue = new LinkedList<>();
                while (!stopFlag) {
                    if (!kafkaOffsetQueue.isEmpty()) {
                        Map<TopicPartition, OffsetAndMetadata> kafkaOffset;
                        while ((kafkaOffset = kafkaOffsetQueue.poll()) != null && !stopFlag) {
                            commitSync(kafkaOffset, kafkaOffsetQueue);
                        }
                    }
                    records = null;
                    try {
                        if (!stopFlag) {
                            records = consumer.poll(Long.MAX_VALUE);
                            if (isPollFailed) {
                                isPollFailed = false;
                                listener.onPollRecovered(topicList);
                            }
                        }
                    } catch (InterruptException ignored) {
                        //do nothing
                    } catch (Exception e) {
                        CONSUMER_ERROR_LOG.error("Poll message failed: `" + e.getMessage() + "`. Assigned partitions: `" + consumer.assignment() +
                                "`. Thread: `" + getName() + "`. Config: `" + config + "`.", e);
                        monitor.onErrorExecution();
                        isPollFailed = true;
                        listener.onPollFailed(topicList);
                        try {
                            Thread.sleep(5000);
                        } catch (InterruptedException ignored) {}
                    }
                    if (records != null) {
                        for (TopicPartition partition : records.partitions()) {
                            String topicName = partition.topic();
                            List<ConsumerRecord<byte[], byte[]>> partitionRecords = records.records(partition);
                            List messageList = new ArrayList();
                            for (ConsumerRecord<byte[], byte[]> record : partitionRecords) {
                                try {
                                    messageList.add(transcoder.decode(record.value()));
                                } catch (TranscoderException e) {
                                    CONSUMER_ERROR_LOG.error("Decode message failed. This partition will be paused. Partition: `" + partition  + "`. Thread: `" + getName()
                                            + "`. Config: `" + config + "`.", e);
                                    monitor.onErrorExecution();
                                    listener.onDecodeMessageFailed(topicName);
                                    messageList = null;
                                    consumer.pause(Collections.singletonList(partition));
                                    break;
                                }
                            }
                            if (messageList != null && !messageList.isEmpty()) {
                                AsyncMessageConsumer asyncMessageConsumer = consumerMap.get(topicName);
                                if (asyncMessageConsumer != null) {
                                    try {
                                        asyncMessageConsumer.consume(messageList);
                                        monitor.onSuccessConsumed(topicName, messageList.size());
                                    } catch (Exception e) { //should not happen
                                        CONSUMER_ERROR_LOG.error("Consume messages failed. This partition will be paused. Messages will not be polled again. Consumer should not throw any exception. Partition: `"
                                                + partition + "`. Thread: `" + getName()  + "`. Config: `" + config + "`. Messages: `" + messageList + "`.", e);
                                        monitor.onErrorExecution();
                                        listener.onConsumeFailed(topicName);
                                        pausePartition(partition);
                                    }
                                    long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
                                    Map<TopicPartition, OffsetAndMetadata> kafkaOffset = Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1));
                                    commitSync(kafkaOffset, kafkaOffsetQueue);
                                } else {
                                    CONSUMER_ERROR_LOG.error("There is no consumer for topic `" + topicName + "`. Thread: `" + getName()
                                            + "`. Config: `" + config + "`. Messages: `" + messageList + "`.");
                                    monitor.onErrorExecution();
                                    listener.onUnrecognizedMessage(topicName);
                                }
                            }
                        }
                    }
                }
            } catch (Exception e) {//should not happen
                CONSUMER_ERROR_LOG.error("Unexpected error. Thread: `" + getName() + "`. Config: `" + config + "`.", e);
            } finally {
                try {
                    consumer.close();
                } catch (Exception e) {
                    CONSUMER_ERROR_LOG.error("Consumer closed failed. Thread: `" + getName() + "`. Config: `" + config + "`.", e);
                }
                CONSUMER_INFO_LOG.info("Kafka consumer thread has been stopped. Thread: `{}`. Config: `{}`.", getName(), config);
            }
        }

        private void commitSync(Map<TopicPartition, OffsetAndMetadata> offset, Queue<Map<TopicPartition, OffsetAndMetadata>> kafkaOffsetQueue) {
            try {
                consumer.commitSync(offset);
            } catch (Exception e) {
                CONSUMER_ERROR_LOG.error("Commit sync failed. Offset: `" + offset + "`. Thread: `" + getName() + "`. Config: `"
                        + config + "`.", e);
                monitor.onErrorExecution();
                kafkaOffsetQueue.offer(offset);
                listener.onCommitSyncFailed(topicList);
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException ignored) {}
            }
        }

        private void pausePartition(TopicPartition partition) {
            try {
                consumer.pause(Collections.singletonList(partition));
            } catch (Exception e) {
                CONSUMER_ERROR_LOG.error("Pause partition failed. Partition: `" + partition + "`. Thread: `" + getName()
                        + "`. Config: `" + config + "`.", e);
            }
        }

        private void close() {
            stopFlag = true;
            interrupt();
        }
    }

    private class KafkaConsumerListenerWrapper implements KafkaConsumerListener {

        private final KafkaConsumerListener listener;

        public KafkaConsumerListenerWrapper(KafkaConsumerListener listener) {
            this.listener = listener;
        }

        @Override
        public void onPollFailed(List<String> subscribedTopics) {
            if (listener != null) {
                try {
                    listener.onPollFailed(subscribedTopics);
                } catch (Exception e) {
                    CONSUMER_ERROR_LOG.error("Call KafkaConsumerListener#onPollFailed() failed. Topics: `" + subscribedTopics
                            + "`. Config: `" + config + "`.", e);
                }
            }
        }

        @Override
        public void onPollRecovered(List<String> subscribedTopics) {
            if (listener != null) {
                try {
                    listener.onPollRecovered(subscribedTopics);
                } catch (Exception e) {
                    CONSUMER_ERROR_LOG.error("Call KafkaConsumerListener#onPollRecovered() failed. Topics: `" + subscribedTopics
                            + "`. Config: `" + config + "`.", e);
                }
            }
        }

        @Override
        public void onDecodeMessageFailed(String topic) {
            if (listener != null) {
                try {
                    listener.onDecodeMessageFailed(topic);
                } catch (Exception e) {
                    CONSUMER_ERROR_LOG.error("Call KafkaConsumerListener#onDecodeMessageFailed() failed. Topic: `" + topic
                            + "`. Config: `" + config + "`.", e);
                }
            }
        }

        @Override
        public void onConsumeFailed(String topic) {
            if (listener != null) {
                try {
                    listener.onConsumeFailed(topic);
                } catch (Exception e) {
                    CONSUMER_ERROR_LOG.error("Call KafkaConsumerListener#onConsumeFailed() failed. Topic: `" + topic
                            + "`. Config: `" + config + "`.", e);
                }
            }
        }

        @Override
        public void onUnrecognizedMessage(String topic) {
            if (listener != null) {
                try {
                    listener.onUnrecognizedMessage(topic);
                } catch (Exception e) {
                    CONSUMER_ERROR_LOG.error("Call KafkaConsumerListener#onUnrecognizedMessage() failed. Topic: `" + topic
                            + "`. Config: `" + config + "`.", e);
                }
            }
        }

        @Override
        public void onCommitSyncFailed(List<String> subscribedTopics) {
            if (listener != null) {
                try {
                    listener.onCommitSyncFailed(subscribedTopics);
                } catch (Exception e) {
                    CONSUMER_ERROR_LOG.error("Call KafkaConsumerListener#onCommitSyncFailed() failed. Topics: `" + subscribedTopics
                            + "`. Config: `" + config + "`.", e);
                }
            }
        }

        @Override
        public void onCommitSyncRecovered(List<String> subscribedTopics) {
            if (listener != null) {
                try {
                    listener.onCommitSyncRecovered(subscribedTopics);
                } catch (Exception e) {
                    CONSUMER_ERROR_LOG.error("Call KafkaConsumerListener#onCommitSyncRecovered() failed. Topics: `" + subscribedTopics
                            + "`. Config: `" + config + "`.", e);
                }
            }
        }
    }
}
