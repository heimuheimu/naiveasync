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

import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka 消费者配置信息，更多内容可参考文档：<a href="http://kafka.apache.org/documentation/#newconsumerconfigs">http://kafka.apache.org/documentation/#newconsumerconfigs</a>。
 *
 * <p><strong>说明：</strong>{@code KafkaConsumerConfig} 类是非线程安全的，不允许多个线程使用同一个实例。</p>
 *
 * @author heimuheimu
 */
public class KafkaConsumerConfig {

    /**
     * Kafka 集群启动地址，例如：host1:port1,h ost2:port2,...
     */
    private String bootstrapServers = "";

    /**
     * Kafka 消费者组 ID
     */
    private String groupId = "";

    /**
     * 单次 Poll 操作返回的最大消息数量
     */
    private int maxPollRecords = 500;

    /**
     * 获得 Kafka 集群启动地址，例如：host1:port1,host2:port2,...。
     *
     * @return Kafka 集群启动地址，例如：host1:port1,host2:port2,...
     */
    public String getBootstrapServers() {
        return bootstrapServers;
    }

    /**
     * 设置 Kafka 集群启动地址，例如：host1:port1,host2:port2,...。
     *
     * @param bootstrapServers Kafka 集群启动地址，例如：host1:port1,host2:port2,...
     */
    public void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    /**
     * 获得 Kafka 消费者组 ID。
     *
     * @return Kafka 消费者组 ID
     */
    public String getGroupId() {
        return groupId;
    }

    /**
     * 设置 Kafka 消费者组 ID。
     *
     * @param groupId Kafka 消费者组 ID
     */
    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    /**
     * 获得单次 Poll 操作返回的最大消息数量。
     *
     * @return 单次 Poll 操作返回的最大消息数量
     */
    public int getMaxPollRecords() {
        return maxPollRecords;
    }

    /**
     * 设置单次 Poll 操作返回的最大消息数量。
     *
     * @param maxPollRecords 单次 Poll 操作返回的最大消息数量
     */
    public void setMaxPollRecords(int maxPollRecords) {
        this.maxPollRecords = maxPollRecords;
    }

    /**
     * 根据当前配置信息返回一个用于构造 {@link org.apache.kafka.clients.consumer.KafkaConsumer} 实例的配置信息 {@code Map}。
     *
     * @return Kafka 配置信息 {@code Map}
     */
    public Map<String, Object> toConfigMap() {
        HashMap<String, Object> configMap = new HashMap<>();
        configMap.put("bootstrap.servers", bootstrapServers);
        configMap.put("key.deserializer", ByteArrayDeserializer.class);
        configMap.put("value.deserializer", ByteArrayDeserializer.class);
        configMap.put("fetch.min.bytes", 1);
        configMap.put("group.id", groupId);
        configMap.put("heartbeat.interval.ms", 3000); // 3 秒
        configMap.put("max.partition.fetch.bytes", 1048576);//每个分区最大获取字节数：1 MB
        configMap.put("session.timeout.ms", 10000); // 10 秒
        configMap.put("auto.offset.reset", "latest");
        configMap.put("connections.max.idle.ms", 540000L); //8 分钟
        configMap.put("enable.auto.commit", false);
        configMap.put("exclude.internal.topics", true);
        configMap.put("fetch.max.bytes", 52428800); //可获取的最大字节数：50 MB
        configMap.put("isolation.level", "read_uncommitted");
        configMap.put("max.poll.interval.ms", 300000); //5 分钟
        configMap.put("max.poll.records", maxPollRecords); //单次 poll 操作返回的最大消息数量：500
        return configMap;
    }

    @Override
    public String toString() {
        return "KafkaConsumerConfig{" +
                "bootstrapServers='" + bootstrapServers + '\'' +
                ", groupId='" + groupId + '\'' +
                ", maxPollRecords=" + maxPollRecords +
                '}';
    }
}
