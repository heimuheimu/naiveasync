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

package com.heimuheimu.async.monitor.producer;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 异步消息生产者监控器
 *
 * @author heimuheimu
 */
public class AsyncMessageProducerMonitor {

    /**
     * 消息累计发送成功次数
     */
    private final AtomicLong totalSuccessCount = new AtomicLong();

    /**
     * 消息累计发送失败次数
     */
    private final AtomicLong totalErrorCount = new AtomicLong();

    /**
     * 指定消息类型累计发送成功次数 Map，Key 为消息类型， Value 为该消息类型累计发送成功次数
     */
    private final ConcurrentHashMap<String, AtomicLong> successCountMap = new ConcurrentHashMap<>();

    /**
     * 指定消息类型累计发送失败次数 Map，Key 为消息类型， Value 为该消息类型累计发送失败次数
     */
    private final ConcurrentHashMap<String, AtomicLong> errorCountMap = new ConcurrentHashMap<>();

    /**
     * 对发送成功的消息进行监控
     *
     * @param messageType 消息类型
     */
    public void onSuccessSent(String messageType) {
        totalSuccessCount.incrementAndGet();
        AtomicLong successCount = successCountMap.get(messageType);
        if (successCount == null) {
            successCount = new AtomicLong();
            successCountMap.put(messageType, successCount);
        }
        successCount.incrementAndGet();
    }

    /**
     * 对发送失败的消息进行监控
     *
     * @param messageType 消息类型
     */
    public void onErrorSent(String messageType) {
        totalErrorCount.incrementAndGet();
        AtomicLong errorCount = errorCountMap.get(messageType);
        if (errorCount == null) {
            errorCount = new AtomicLong();
            errorCountMap.put(messageType, errorCount);
        }
        errorCount.incrementAndGet();
    }

    /**
     * 获得消息累计发送成功次数
     *
     * @return 消息累计发送成功次数
     */
    public long getTotalSuccessCount() {
        return totalSuccessCount.get();
    }

    /**
     * 获得消息累计发送失败次数
     *
     * @return 消息累计发送失败次数
     */
    public long getTotalErrorCount() {
        return totalErrorCount.get();
    }

    /**
     * 获得指定消息类型的累计发送成功次数
     *
     * @param messageType 消息类型
     * @return 指定消息类型的累计发送成功次数
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
     * 获得指定消息类型的累计发送失败次数
     *
     * @param messageType 消息类型
     * @return 指定消息类型的累计发送失败次数
     */
    public long getErrorCount(String messageType) {
        AtomicLong errorCount = errorCountMap.get(messageType);
        if (errorCount != null) {
            return errorCount.get();
        } else {
            return 0;
        }
    }

    @Override
    public String toString() {
        return "AsyncMessageProducerMonitor{" +
                "totalSuccessCount=" + totalSuccessCount +
                ", totalErrorCount=" + totalErrorCount +
                ", successCountMap=" + successCountMap +
                ", errorCountMap=" + errorCountMap +
                '}';
    }
}
