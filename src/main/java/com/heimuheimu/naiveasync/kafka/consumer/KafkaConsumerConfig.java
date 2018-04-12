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
     *
     * <p>
     * A list of host/port pairs to use for establishing the initial connection to the Kafka cluster.
     * The client will make use of all servers irrespective of which servers are specified here for bootstrapping—this list
     * only impacts the initial hosts used to discover the full set of servers. This list should be in the
     * form host1:port1,host2:port2,.... Since these servers are just used for the initial connection to discover the
     * full cluster membership (which may change dynamically), this list need not contain the full set of servers
     * (you may want more than one, though, in case a server is down).
     * </p>
     */
    private String bootstrapServers = "";

    /**
     * 每次消息获取请求 Kafka 服务返回的最小字节数，默认为 1
     *
     * <p>
     * The minimum amount of data the server should return for a fetch request.
     * If insufficient data is available the request will wait for that much data to accumulate before answering the request.
     * The default setting of 1 byte means that fetch requests are answered as soon as a single byte of data is available
     * or the fetch request times out waiting for data to arrive. Setting this to something greater than 1 will cause
     * the server to wait for larger amounts of data to accumulate which can improve server throughput a bit at the cost
     * of some additional latency.
     * </p>
     */
    private int fetchMinBytes = 1;

    /**
     * Kafka 消费者组 ID
     *
     * <p>
     * A unique string that identifies the consumer group this consumer belongs to. This property is required if the consumer
     * uses either the group management functionality by using subscribe(topic) or the Kafka-based offset management strategy.
     * </p>
     */
    private String groupId = "";

    /**
     * 消费者心跳检测间隔时间，默认为 3 秒，用来确保消费者 session 保持活跃，该值必须比 sessionTimeoutMs 的值小，通常不高于 sessionTimeoutMs 值的 1/3
     *
     * <p>
     * The expected time between heartbeats to the consumer coordinator when using Kafka's group management facilities.
     * Heartbeats are used to ensure that the consumer's session stays active and to facilitate rebalancing when new consumers
     * join or leave the group. The value must be set lower than session.timeout.ms, but typically should be set no higher
     * than 1/3 of that value. It can be adjusted even lower to control the expected time for normal rebalances.
     * </p>
     */
    private int heartbeatIntervalMs = 3000;

    /**
     * 每个分区最大获取字节数，默认为 1 MB
     *
     * <p>
     * The maximum amount of data per-partition the server will return. Records are fetched in batches by the consumer.
     * If the first record batch in the first non-empty partition of the fetch is larger than this limit,
     * the batch will still be returned to ensure that the consumer can make progress. The maximum record batch size accepted
     * by the broker is defined via message.max.bytes (broker config) or max.message.bytes (topic config).
     * See fetch.max.bytes for limiting the consumer request size.
     * </p>
     */
    private int maxPartitionFetchBytes = 1048576;

    /**
     * 消费者超时时间，默认为 10 秒，如果在该时间内 Kafka 服务没有接收到心跳请求，该消费者将会从消费组中移除。
     *
     * <p>
     * The timeout used to detect consumer failures when using Kafka's group management facility. The consumer sends
     * periodic heartbeats to indicate its liveness to the broker. If no heartbeats are received by the broker before
     * the expiration of this session timeout, then the broker will remove this consumer from the group and initiate
     * a rebalance. Note that the value must be in the allowable range as configured in the broker configuration
     * by group.min.session.timeout.ms and group.max.session.timeout.ms.
     * </p>
     */
    private int sessionTimeoutMs = 10000;

    /**
     * 默认断点策略，默认为 "latest"，允许的值为：latest、earliest、none
     *
     * <p>
     * What to do when there is no initial offset in Kafka or if the current offset does not exist any more on the server (e.g. because that data has been deleted):
     * <ul>
     * <li>earliest: automatically reset the offset to the earliest offset</li>
     * <li>latest: automatically reset the offset to the latest offset</li>
     * <li>none: throw exception to the consumer if no previous offset is found for the consumer's group</li>
     * <li>anything else: throw exception to the consumer.</li>
     * </ul>
     * </p>
     */
    private String autoOffsetReset = "latest";

    /**
     * 连接最大空闲时间，默认为 8 分钟
     *
     * <p>
     * Close idle connections after the number of milliseconds specified by this config.
     * </p>
     */
    private long connectionsMaxIdleMs = 540000;

    /**
     * 是否允许消费者自动提交断点，默认为 false
     *
     * <p>
     * If true the consumer's offset will be periodically committed in the background.
     * </p>
     */
    private boolean enableAutoCommit = false;

    /**
     * 是否不允许消费者消费内部 Topic，默认为 true
     *
     * <p>
     * Whether records from internal topics (such as offsets) should be exposed to the consumer. If set to true the
     * only way to receive records from an internal topic is subscribing to it.
     * </p>
     */
    private boolean excludeInternalTopics = true;

    /**
     * 每次消息获取请求 Kafka 服务返回的最大字节数，默认为 50 MB
     *
     * <p>
     * The maximum amount of data the server should return for a fetch request. Records are fetched in batches by the consumer,
     * and if the first record batch in the first non-empty partition of the fetch is larger than this value, the record batch
     * will still be returned to ensure that the consumer can make progress. As such, this is not a absolute maximum.
     * The maximum record batch size accepted by the broker is defined via message.max.bytes (broker config)
     * or max.message.bytes (topic config). Note that the consumer performs multiple fetches in parallel.
     * </p>
     */
    private int fetchMaxBytes = 52428800;

    /**
     * 读取消息策略，默认为 "read_uncommitted"，允许的值为：read_committed、read_uncommitted
     *
     * <p>
     * Controls how to read messages written transactionally. If set to read_committed, consumer.poll() will only return
     * transactional messages which have been committed. If set to read_uncommitted' (the default), consumer.poll() will
     * return all messages, even transactional messages which have been aborted. Non-transactional messages will be returned
     * unconditionally in either mode.
     * <br><br>Messages will always be returned in offset order. Hence, in read_committed mode, consumer.poll() will
     * only return messages up to the last stable offset (LSO), which is the one less than the offset of the first open
     * transaction. In particular any messages appearing after messages belonging to ongoing transactions will be
     * withheld until the relevant transaction has been completed. As a result, read_committed consumers will not be able
     * to read up to the high watermark when there are in flight transactions.
     * <br><br>Further, when in read_committed the seekToEnd method will return the LSO
     * </p>
     */
    private String isolationLevel = "read_uncommitted";

    /**
     * 两次 Poll 操作的最大时间间隔，默认为 5 分钟
     *
     * <p>
     * The maximum delay between invocations of poll() when using consumer group management. This places an upper bound
     * on the amount of time that the consumer can be idle before fetching more records. If poll() is not called before
     * expiration of this timeout, then the consumer is considered failed and the group will rebalance in order to reassign
     * the partitions to another member.
     * </p>
     */
    private int maxPollIntervalMs = 300000;

    /**
     * 单次 Poll 操作返回的最大消息数量，默认为 500
     *
     * <p>
     * The maximum number of records returned in a single call to poll().
     * </p>
     */
    private int maxPollRecords = 500;

    /**
     * Socket 读取缓存大小，默认为 64 KB
     *
     * <p>
     * The size of the TCP receive buffer (SO_RCVBUF) to use when reading data. If the value is -1, the OS default will be used.
     * </p>
     */
    private int receiveBufferBytes = 65536;

    /**
     * 请求超时时间，默认为 305 秒
     *
     * <p>
     * The configuration controls the maximum amount of time the client will wait for the response of a request.
     * If the response is not received before the timeout elapses the client will resend the request if necessary or
     * fail the request if retries are exhausted.
     * </p>
     */
    private int requestTimeoutMs = 305000;

    /**
     * Socket 写入缓存大小，默认为 128 KB
     *
     * <p>
     * The size of the TCP send buffer (SO_SNDBUF) to use when sending data. If the value is -1, the OS default will be used.
     * </p>
     */
    private int sendBufferBytes = 131072;

    /**
     * 消费者 ID，用于在 Kafka 服务中追踪请求来源，默认为空字符串
     *
     * <p>
     * An id string to pass to the server when making requests. The purpose of this is to be able to track the source of
     * requests beyond just ip/port by allowing a logical application name to be included in server-side request logging.
     * </p>
     */
    private String clientId = "";

    /**
     * 获得 Kafka 集群启动地址，例如：host1:port1,h ost2:port2,...
     *
     * @return Kafka 集群启动地址，例如：host1:port1,h ost2:port2,...
     */
    public String getBootstrapServers() {
        return bootstrapServers;
    }

    /**
     * 设置 Kafka 集群启动地址，例如：host1:port1,h ost2:port2,...
     *
     * <p>
     * A list of host/port pairs to use for establishing the initial connection to the Kafka cluster.
     * The client will make use of all servers irrespective of which servers are specified here for bootstrapping—this list
     * only impacts the initial hosts used to discover the full set of servers. This list should be in the
     * form host1:port1,host2:port2,.... Since these servers are just used for the initial connection to discover the
     * full cluster membership (which may change dynamically), this list need not contain the full set of servers
     * (you may want more than one, though, in case a server is down).
     * </p>
     *
     * @param bootstrapServers Kafka 集群启动地址，例如：host1:port1,h ost2:port2,...
     */
    public void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    /**
     * 获得每次消息获取请求 Kafka 服务返回的最小字节数，默认为 1 字节。
     *
     * @return 每次消息获取请求 Kafka 服务返回的最小字节数
     */
    public int getFetchMinBytes() {
        return fetchMinBytes;
    }

    /**
     * 设置每次消息获取请求 Kafka 服务返回的最小字节数。
     *
     * <p>
     * The minimum amount of data the server should return for a fetch request.
     * If insufficient data is available the request will wait for that much data to accumulate before answering the request.
     * The default setting of 1 byte means that fetch requests are answered as soon as a single byte of data is available
     * or the fetch request times out waiting for data to arrive. Setting this to something greater than 1 will cause
     * the server to wait for larger amounts of data to accumulate which can improve server throughput a bit at the cost
     * of some additional latency.
     * </p>
     *
     * @param fetchMinBytes 每次消息获取请求 Kafka 服务返回的最小字节数
     */
    public void setFetchMinBytes(int fetchMinBytes) {
        this.fetchMinBytes = fetchMinBytes;
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
     * <p>
     * A unique string that identifies the consumer group this consumer belongs to. This property is required if the consumer
     * uses either the group management functionality by using subscribe(topic) or the Kafka-based offset management strategy.
     * </p>
     *
     * @param groupId Kafka 消费者组 ID
     */
    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    /**
     * 获得消费者心跳检测间隔时间，默认为 3000 毫秒（3 秒），用来确保消费者 session 保持活跃。
     *
     * @return 消费者心跳检测间隔时间
     */
    public int getHeartbeatIntervalMs() {
        return heartbeatIntervalMs;
    }

    /**
     * 设置消费者心跳检测间隔时间，单位：毫秒，用来确保消费者 session 保持活跃，该值必须比 sessionTimeoutMs 的值小，
     * 通常不高于 sessionTimeoutMs 值的 1/3。
     *
     * <p>
     * The expected time between heartbeats to the consumer coordinator when using Kafka's group management facilities.
     * Heartbeats are used to ensure that the consumer's session stays active and to facilitate rebalancing when new consumers
     * join or leave the group. The value must be set lower than session.timeout.ms, but typically should be set no higher
     * than 1/3 of that value. It can be adjusted even lower to control the expected time for normal rebalances.
     * </p>
     *
     * @param heartbeatIntervalMs 心跳检测间隔时间
     */
    public void setHeartbeatIntervalMs(int heartbeatIntervalMs) {
        this.heartbeatIntervalMs = heartbeatIntervalMs;
    }

    /**
     * 获得每个分区最大获取字节数，默认为 1048576 字节（1 MB）。
     *
     * @return 每个分区最大获取字节数
     */
    public int getMaxPartitionFetchBytes() {
        return maxPartitionFetchBytes;
    }

    /**
     * 设置每个分区最大获取字节数。
     *
     * <p>
     * The maximum amount of data per-partition the server will return. Records are fetched in batches by the consumer.
     * If the first record batch in the first non-empty partition of the fetch is larger than this limit,
     * the batch will still be returned to ensure that the consumer can make progress. The maximum record batch size accepted
     * by the broker is defined via message.max.bytes (broker config) or max.message.bytes (topic config).
     * See fetch.max.bytes for limiting the consumer request size.
     * </p>
     *
     * @param maxPartitionFetchBytes 每个分区最大获取字节数
     */
    public void setMaxPartitionFetchBytes(int maxPartitionFetchBytes) {
        this.maxPartitionFetchBytes = maxPartitionFetchBytes;
    }

    /**
     * 获得消费者超时时间，默认为 10000 毫秒（10 秒），如果在该时间内 Kafka 服务没有接收到心跳请求，该消费者将会从消费组中移除。
     *
     * @return 消费者超时时间
     */
    public int getSessionTimeoutMs() {
        return sessionTimeoutMs;
    }

    /**
     * 设置消费者超时时间，单位：毫秒，如果在该时间内 Kafka 服务没有接收到心跳请求，该消费者将会从消费组中移除。
     *
     * <p>
     * The timeout used to detect consumer failures when using Kafka's group management facility. The consumer sends
     * periodic heartbeats to indicate its liveness to the broker. If no heartbeats are received by the broker before
     * the expiration of this session timeout, then the broker will remove this consumer from the group and initiate
     * a rebalance. Note that the value must be in the allowable range as configured in the broker configuration
     * by group.min.session.timeout.ms and group.max.session.timeout.ms.
     * </p>
     *
     * @param sessionTimeoutMs 消费者超时时间
     */
    public void setSessionTimeoutMs(int sessionTimeoutMs) {
        this.sessionTimeoutMs = sessionTimeoutMs;
    }

    /**
     * 获得默认断点策略，默认为 "latest"，允许的值为：latest、earliest、none。
     *
     * @return 默认断点策略
     */
    public String getAutoOffsetReset() {
        return autoOffsetReset;
    }

    /**
     * 设置默认断点策略，允许的值为：latest、earliest、none。
     *
     * <p>
     * What to do when there is no initial offset in Kafka or if the current offset does not exist any more on the server (e.g. because that data has been deleted):
     * </p>
     * <ul>
     * <li>earliest: automatically reset the offset to the earliest offset</li>
     * <li>latest: automatically reset the offset to the latest offset</li>
     * <li>none: throw exception to the consumer if no previous offset is found for the consumer's group</li>
     * <li>anything else: throw exception to the consumer.</li>
     * </ul>
     *
     *
     * @param autoOffsetReset 默认断点策略
     */
    public void setAutoOffsetReset(String autoOffsetReset) {
        this.autoOffsetReset = autoOffsetReset;
    }

    /**
     * 获得连接最大空闲时间，默认为 540000 毫秒（ 8 分钟）。
     *
     * @return 连接最大空闲时间
     */
    public long getConnectionsMaxIdleMs() {
        return connectionsMaxIdleMs;
    }

    /**
     * 设置连接最大空闲时间，单位：毫秒。
     *
     * <p>
     * Close idle connections after the number of milliseconds specified by this config.
     * </p>
     *
     * @param connectionsMaxIdleMs 连接最大空闲时间
     */
    public void setConnectionsMaxIdleMs(long connectionsMaxIdleMs) {
        this.connectionsMaxIdleMs = connectionsMaxIdleMs;
    }

    /**
     * 判断是否允许消费者自动提交断点，默认为 false。
     *
     * @return 是否允许消费者自动提交断点
     */
    public boolean isEnableAutoCommit() {
        return enableAutoCommit;
    }

    /**
     * 设置是否允许消费者自动提交断点。
     *
     * <p>
     * If true the consumer's offset will be periodically committed in the background.
     * </p>
     *
     * @param enableAutoCommit 是否允许消费者自动提交断点
     */
    public void setEnableAutoCommit(boolean enableAutoCommit) {
        this.enableAutoCommit = enableAutoCommit;
    }

    /**
     * 判断是否不允许消费者消费内部 Topic。
     *
     * @return 是否不允许消费者消费内部 Topic
     */
    public boolean isExcludeInternalTopics() {
        return excludeInternalTopics;
    }

    /**
     * 设置是否不允许消费者消费内部 Topic。
     *
     * <p>
     * Whether records from internal topics (such as offsets) should be exposed to the consumer. If set to true the
     * only way to receive records from an internal topic is subscribing to it.
     * </p>
     *
     * @param excludeInternalTopics 是否不允许消费者消费内部 Topic
     */
    public void setExcludeInternalTopics(boolean excludeInternalTopics) {
        this.excludeInternalTopics = excludeInternalTopics;
    }

    /**
     * 获得每次消息获取请求 Kafka 服务返回的最大字节数，默认为 52428800 字节（50 MB）。
     *
     * @return 每次消息获取请求 Kafka 服务返回的最大字节数
     */
    public int getFetchMaxBytes() {
        return fetchMaxBytes;
    }

    /**
     * 设置每次消息获取请求 Kafka 服务返回的最大字节数。
     *
     * <p>
     * The maximum amount of data the server should return for a fetch request. Records are fetched in batches by the consumer,
     * and if the first record batch in the first non-empty partition of the fetch is larger than this value, the record batch
     * will still be returned to ensure that the consumer can make progress. As such, this is not a absolute maximum.
     * The maximum record batch size accepted by the broker is defined via message.max.bytes (broker config)
     * or max.message.bytes (topic config). Note that the consumer performs multiple fetches in parallel.
     * </p>
     *
     * @param fetchMaxBytes 每次消息获取请求 Kafka 服务返回的最大字节数
     */
    public void setFetchMaxBytes(int fetchMaxBytes) {
        this.fetchMaxBytes = fetchMaxBytes;
    }

    /**
     * 获得读取消息策略，默认为 "read_uncommitted"，允许的值为：read_committed、read_uncommitted。
     *
     * @return 读取消息策略
     */
    public String getIsolationLevel() {
        return isolationLevel;
    }

    /**
     * 设置读取消息策略，允许的值为：read_committed、read_uncommitted。
     *
     * <p>
     * Controls how to read messages written transactionally. If set to read_committed, consumer.poll() will only return
     * transactional messages which have been committed. If set to read_uncommitted' (the default), consumer.poll() will
     * return all messages, even transactional messages which have been aborted. Non-transactional messages will be returned
     * unconditionally in either mode.
     * <br><br>Messages will always be returned in offset order. Hence, in read_committed mode, consumer.poll() will
     * only return messages up to the last stable offset (LSO), which is the one less than the offset of the first open
     * transaction. In particular any messages appearing after messages belonging to ongoing transactions will be
     * withheld until the relevant transaction has been completed. As a result, read_committed consumers will not be able
     * to read up to the high watermark when there are in flight transactions.
     * <br><br>Further, when in read_committed the seekToEnd method will return the LSO
     * </p>
     *
     * @param isolationLevel 读取消息策略
     */
    public void setIsolationLevel(String isolationLevel) {
        this.isolationLevel = isolationLevel;
    }

    /**
     * 获得两次 Poll 操作的最大时间间隔，默认为 300000 毫秒（5 分钟）。
     *
     * @return 两次 Poll 操作的最大时间间隔
     */
    public int getMaxPollIntervalMs() {
        return maxPollIntervalMs;
    }

    /**
     * 设置两次 Poll 操作的最大时间间隔，单位：毫秒。
     *
     * <p>
     * The maximum delay between invocations of poll() when using consumer group management. This places an upper bound
     * on the amount of time that the consumer can be idle before fetching more records. If poll() is not called before
     * expiration of this timeout, then the consumer is considered failed and the group will rebalance in order to reassign
     * the partitions to another member.
     * </p>
     *
     * @param maxPollIntervalMs 两次 Poll 操作的最大时间间隔
     */
    public void setMaxPollIntervalMs(int maxPollIntervalMs) {
        this.maxPollIntervalMs = maxPollIntervalMs;
    }

    /**
     * 获得单次 Poll 操作返回的最大消息数量，默认为 500 条。
     *
     * @return 单次 Poll 操作返回的最大消息数量
     */
    public int getMaxPollRecords() {
        return maxPollRecords;
    }

    /**
     * 设置单次 Poll 操作返回的最大消息数量。
     *
     * <p>
     * The maximum number of records returned in a single call to poll().
     * </p>
     *
     * @param maxPollRecords 单次 Poll 操作返回的最大消息数量
     */
    public void setMaxPollRecords(int maxPollRecords) {
        this.maxPollRecords = maxPollRecords;
    }

    /**
     * 获得 Socket 读取缓存大小，默认为 65536 字节（64KB）。
     *
     * @return Socket 读取缓存大小
     */
    public int getReceiveBufferBytes() {
        return receiveBufferBytes;
    }

    /**
     * 设置 Socket 读取缓存大小。
     *
     * <p>
     * The size of the TCP receive buffer (SO_RCVBUF) to use when reading data. If the value is -1, the OS default will be used.
     * </p>
     *
     * @param receiveBufferBytes Socket 读取缓存大小
     */
    public void setReceiveBufferBytes(int receiveBufferBytes) {
        this.receiveBufferBytes = receiveBufferBytes;
    }

    /**
     * 获得请求超时时间，默认为 305000 毫秒（305 秒）。
     *
     * @return 请求超时时间
     */
    public int getRequestTimeoutMs() {
        return requestTimeoutMs;
    }

    /**
     * 设置请求超时时间，单位：毫秒。
     *
     * <p>
     * The configuration controls the maximum amount of time the client will wait for the response of a request.
     * If the response is not received before the timeout elapses the client will resend the request if necessary or
     * fail the request if retries are exhausted.
     * </p>
     *
     * @param requestTimeoutMs 请求超时时间
     */
    public void setRequestTimeoutMs(int requestTimeoutMs) {
        this.requestTimeoutMs = requestTimeoutMs;
    }

    /**
     * 获得 Socket 写入缓存大小，默认为 131072 字节（128 KB）。
     *
     * @return Socket 写入缓存大小
     */
    public int getSendBufferBytes() {
        return sendBufferBytes;
    }

    /**
     * 设置 Socket 写入缓存大小。
     *
     * <p>
     * The size of the TCP send buffer (SO_SNDBUF) to use when sending data. If the value is -1, the OS default will be used.
     * </p>
     *
     * @param sendBufferBytes Socket 写入缓存大小
     */
    public void setSendBufferBytes(int sendBufferBytes) {
        this.sendBufferBytes = sendBufferBytes;
    }

    /**
     * 获得消费者 ID，用于在 Kafka 服务中追踪请求来源，默认为空字符串。
     *
     * @return 消费者 ID
     */
    public String getClientId() {
        return clientId;
    }

    /**
     * 设置消费者 ID，用于在 Kafka 服务中追踪请求来源。
     *
     * <p>
     * An id string to pass to the server when making requests. The purpose of this is to be able to track the source of
     * requests beyond just ip/port by allowing a logical application name to be included in server-side request logging.
     * </p>
     *
     * @param clientId 消费者 ID
     */
    public void setClientId(String clientId) {
        this.clientId = clientId;
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
        configMap.put("fetch.min.bytes", fetchMinBytes);
        configMap.put("group.id", groupId);
        configMap.put("heartbeat.interval.ms", heartbeatIntervalMs);
        configMap.put("max.partition.fetch.bytes", maxPartitionFetchBytes);
        configMap.put("session.timeout.ms", sessionTimeoutMs);
        configMap.put("auto.offset.reset", autoOffsetReset);
        configMap.put("connections.max.idle.ms", connectionsMaxIdleMs);
        configMap.put("enable.auto.commit", enableAutoCommit);
        configMap.put("exclude.internal.topics", excludeInternalTopics);
        configMap.put("fetch.max.bytes", fetchMaxBytes);
        configMap.put("isolation.level", isolationLevel);
        configMap.put("max.poll.interval.ms", maxPollIntervalMs);
        configMap.put("max.poll.records", maxPollRecords);
        configMap.put("receive.buffer.bytes", receiveBufferBytes);
        configMap.put("request.timeout.ms", requestTimeoutMs);
        configMap.put("send.buffer.bytes", sendBufferBytes);
        configMap.put("client.id", clientId);
        return configMap;
    }

    @Override
    public String toString() {
        return "KafkaConsumerConfig{" +
                "bootstrapServers='" + bootstrapServers + '\'' +
                ", fetchMinBytes=" + fetchMinBytes +
                ", groupId='" + groupId + '\'' +
                ", heartbeatIntervalMs=" + heartbeatIntervalMs +
                ", maxPartitionFetchBytes=" + maxPartitionFetchBytes +
                ", sessionTimeoutMs=" + sessionTimeoutMs +
                ", autoOffsetReset='" + autoOffsetReset + '\'' +
                ", connectionsMaxIdleMs=" + connectionsMaxIdleMs +
                ", enableAutoCommit=" + enableAutoCommit +
                ", excludeInternalTopics=" + excludeInternalTopics +
                ", fetchMaxBytes=" + fetchMaxBytes +
                ", isolationLevel='" + isolationLevel + '\'' +
                ", maxPollIntervalMs=" + maxPollIntervalMs +
                ", maxPollRecords=" + maxPollRecords +
                ", receiveBufferBytes=" + receiveBufferBytes +
                ", requestTimeoutMs=" + requestTimeoutMs +
                ", sendBufferBytes=" + sendBufferBytes +
                ", clientId='" + clientId + '\'' +
                '}';
    }
}
