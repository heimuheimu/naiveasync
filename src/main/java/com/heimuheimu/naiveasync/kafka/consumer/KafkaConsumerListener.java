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

import org.apache.kafka.common.TopicPartition;

/**
 * Kafka 消费者事件监听器。
 *
 * <p>
 *     <strong>说明：</strong>监听器的实现类必须是线程安全的。应优先考虑继承 {@link KafkaConsumerListenerSkeleton} 骨架类进行实现，
 *     防止 {@code KafkaConsumerListener} 在后续版本增加监听事件时，带来的编译错误。
 * </p>
 *
 * @author heimuheimu
 */
public interface KafkaConsumerListener {

    /**
     * 当拉取消息失败时，将触发此事件。
     *
     * @param bootstrapServers Kafka 集群启动地址，例如：host1:port1,host2:port2,...
     */
    void onPollFailed(String bootstrapServers);

    /**
     * 当拉取消息失败后，又重新恢复，将触发此事件。
     *
     * @param bootstrapServers Kafka 集群启动地址，例如：host1:port1,host2:port2,...
     */
    void onPollRecovered(String bootstrapServers);

    /**
     * 当消费失败时，将触发此事件。
     *
     * @param partition 消息所在 Kafka 分区信息，不会为 {@code null}
     * @param message 消费失败的消息，如果在消息反序列化过程中出现错误，该值为 {@code null}
     * @param bootstrapServers Kafka 集群启动地址，例如：host1:port1,host2:port2,...
     */
    void onConsumeFailed(TopicPartition partition, Object message, String bootstrapServers);

    /**
     * 当 Kafka 断点提交失败时，将触发此事件。
     *
     * @param partition 提交断点失败的 Kafka 分区信息
     * @param bootstrapServers Kafka 集群启动地址，例如：host1:port1,host2:port2,...
     */
    void onCommitSyncFailed(TopicPartition partition, String bootstrapServers);

    /**
     * 当 Kafka 断点提交失败后，又重新恢复，将触发此事件。
     *
     * @param partition 提交断点恢复的 Kafka 分区信息
     * @param bootstrapServers Kafka 集群启动地址，例如：host1:port1,host2:port2,...
     */
    void onCommitSyncRecovered(TopicPartition partition, String bootstrapServers);

    /**
     * 当停止拉取 Kafka 分区中的消息时，将触发此事件。
     *
     * @param partition 停止拉取的 Kafka 分区信息
     * @param bootstrapServers Kafka 集群启动地址，例如：host1:port1,host2:port2,...
     */
    void onPartitionPaused(TopicPartition partition, String bootstrapServers);
}
