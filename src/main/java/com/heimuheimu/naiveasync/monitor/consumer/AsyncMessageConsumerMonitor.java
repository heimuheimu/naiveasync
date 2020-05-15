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

package com.heimuheimu.naiveasync.monitor.consumer;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 异步消息消费者监控器。
 *
 * @author heimuheimu
 */
public class AsyncMessageConsumerMonitor {

    /**
     * 已接收到的消息总数
     */
    private final AtomicLong totalPolledCount = new AtomicLong();

    /**
     * 已接收到的消息延迟总时间，单位：毫秒
     */
    private final AtomicLong totalDelayedMills = new AtomicLong();

    /**
     * 已接收到的消息最大延迟时间，单位：毫秒
     */
    private volatile long maxDelayedMills = 0;

    /**
     * 指定消息类型已接收到的消息总数 Map，Key 为消息类型， Value 为该消息类型累计接收到的消息总量
     */
    private final ConcurrentHashMap<String, AtomicLong> polledCountMap = new ConcurrentHashMap<>();

    /**
     * 指定消息类型已接收到的消息延迟总时间 Map，Key 为消息类型， Value 为该消息类型已接收到的消息延迟总时间，单位：毫秒
     */
    private final ConcurrentHashMap<String, AtomicLong> delayedMillsMap = new ConcurrentHashMap<>();

    /**
     * 指定消息类型已接收到的消息最大延迟时间 Map，Key 为消息类型， Value 为该消息类型已接收到的消息最大延迟时间，单位：毫秒
     */
    private final ConcurrentHashMap<String, Long> specificMaxDelayedMillsMap = new ConcurrentHashMap<>();

    /**
     * 消息累计消费成功次数
     */
    private final AtomicLong totalSuccessCount = new AtomicLong();

    /**
     * 指定消息类型累计消费成功次数 Map，Key 为消息类型， Value 为该消息类型累计消费成功次数
     */
    private final ConcurrentHashMap<String, AtomicLong> successCountMap = new ConcurrentHashMap<>();

    /**
     * 消费过程中发生 ERROR 的次数
     */
    private final AtomicLong executionErrorCount = new AtomicLong();

    /**
     * 在消息到达时，对消息延迟时间进行监控。
     *
     * @param messageType 消息类型
     * @param delayMills 消息延迟时间，单位：毫秒
     */
    public void onPolled(String messageType, long delayMills) {
        totalPolledCount.incrementAndGet();
        totalDelayedMills.addAndGet(delayMills);
        if (delayMills > maxDelayedMills) {
            maxDelayedMills = delayMills;
        }

        AtomicLong polledCount = polledCountMap.get(messageType);
        if (polledCount == null) {
            polledCount = new AtomicLong();
            polledCountMap.put(messageType, polledCount);
        }
        polledCount.incrementAndGet();

        AtomicLong specificDelayedMills = delayedMillsMap.get(messageType);
        if (specificDelayedMills == null) {
            specificDelayedMills = new AtomicLong();
            delayedMillsMap.put(messageType, specificDelayedMills);
        }
        specificDelayedMills.addAndGet(delayMills);

        Long specificMaxDelayedMills = specificMaxDelayedMillsMap.get(messageType);
        if (specificMaxDelayedMills == null || delayMills > specificMaxDelayedMills) {
            specificMaxDelayedMillsMap.put(messageType, delayMills);
        }
    }

    /**
     * 对消费成功的消息进行监控。
     *
     * @param messageType 消息类型
     * @param count 成功消费的次数
     */
    public void onSuccessConsumed(String messageType, int count) {
        totalSuccessCount.addAndGet(count);
        AtomicLong successCount = successCountMap.get(messageType);
        if (successCount == null) {
            successCount = new AtomicLong();
            successCountMap.put(messageType, successCount);
        }
        successCount.addAndGet(count);
    }

    /**
     * 对消费过程中发生 ERROR 进行监控。
     */
    public void onErrorExecution() {
        executionErrorCount.incrementAndGet();
    }

    /**
     * 获取已接收到的消息总数。
     *
     * @return 已接收到的消息总数
     */
    public long getTotalPolledCount() {
        return totalPolledCount.get();
    }

    /**
     * 获得指定消息类型已接收到的消息总数。
     *
     * @param messageType 消息类型
     * @return 指定消息类型已接收到的消息总数
     */
    public long getPolledCount(String messageType) {
        AtomicLong polledCount = polledCountMap.get(messageType);
        if (polledCount != null) {
            return polledCount.get();
        } else {
            return 0;
        }
    }

    /**
     * 获取已接收到的消息延迟总时间，单位：毫秒。
     *
     * @return 已接收到的消息延迟总时间，单位：毫秒
     */
    public long getTotalDelayedMills() {
        return totalDelayedMills.get();
    }

    /**
     * 获取指定消息类型已接收到的消息延迟总时间，单位：毫秒。
     *
     * @param messageType 消息类型
     * @return 指定消息类型已接收到的消息延迟总时间，单位：毫秒
     */
    public long getDelayedMills(String messageType) {
        AtomicLong delayedMills = delayedMillsMap.get(messageType);
        if (delayedMills != null) {
            return delayedMills.get();
        } else {
            return 0;
        }
    }

    /**
     * 获得已接收到的消息最大延迟时间，单位：毫秒。
     *
     * @return 已接收到的消息最大延迟时间，单位：毫秒
     */
    public long getMaxDelayedMills() {
        return maxDelayedMills;
    }

    /**
     * 获得指定消息类型已接收到的消息最大延迟时间，单位：毫秒。
     *
     * @param messageType 消息类型
     * @return 指定消息类型已接收到的消息最大延迟时间，单位：毫秒
     */
    public long getSpecificMaxDelayedMills(String messageType) {
        Long specificDelayedMills = specificMaxDelayedMillsMap.get(messageType);
        if (specificDelayedMills != null) {
            return specificDelayedMills;
        } else {
            return 0;
        }
    }

    /**
     * 获得消息累计消费成功次数。
     *
     * @return 消息累计消费成功次数
     */
    public long getTotalSuccessCount() {
        return totalSuccessCount.get();
    }

    /**
     * 获得指定消息类型的累计消费成功次数。
     *
     * @param messageType 消息类型
     * @return 指定消息类型的累计消费成功次数
     */
    public long getSuccessCount(String messageType) {
        AtomicLong successCount = successCountMap.get(messageType);
        if (successCount != null) {
            return successCount.get();
        } else {
            return 0;
        }
    }

    /**
     * 获得消费过程中发生 ERROR 的次数。
     *
     * @return 消费过程中发生 ERROR 的次数
     */
    public long getExecutionErrorCount() {
        return executionErrorCount.get();
    }

    /**
     * 获得消息类型列表，不会返回 {@code null}。
     *
     * @return 消息类型列表，不会为 {@code null}
     * @since 1.2
     */
    public Set<String> getMessageTypeSet() {
        return new HashSet<>(polledCountMap.keySet());
    }

    /**
     * 重置已接收到的消息最大延迟时间，单位：毫秒。
     */
    public void resetMaxDelayedMills() {
        maxDelayedMills = 0;
    }

    /**
     * 重置指定消息类型已接收到的消息最大延迟时间，单位：毫秒。
     *
     * @param messageType 消息类型
     */
    public void resetSpecificMaxDelayedMills(String messageType) {
        specificMaxDelayedMillsMap.remove(messageType);
    }

    @Override
    public String toString() {
        return "AsyncMessageConsumerMonitor{" +
                "totalPolledCount=" + totalPolledCount +
                ", totalDelayedMills=" + totalDelayedMills +
                ", maxDelayedMills=" + maxDelayedMills +
                ", polledCountMap=" + polledCountMap +
                ", delayedMillsMap=" + delayedMillsMap +
                ", specificMaxDelayedMillsMap=" + specificMaxDelayedMillsMap +
                ", totalSuccessCount=" + totalSuccessCount +
                ", successCountMap=" + successCountMap +
                ", executionErrorCount=" + executionErrorCount +
                '}';
    }
}
