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

package com.heimuheimu.naiveasync.monitor.consumer.falcon;

import com.heimuheimu.naiveasync.monitor.consumer.AsyncMessageConsumerMonitor;
import com.heimuheimu.naiveasync.monitor.consumer.AsyncMessageConsumerMonitorFactory;
import com.heimuheimu.naivemonitor.falcon.FalconData;
import com.heimuheimu.naivemonitor.falcon.support.AbstractFalconDataCollector;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 异步消息消费者监控数据采集器
 *
 * @author heimuheimu
 */
public class AsyncMessageConsumerDataCollector extends AbstractFalconDataCollector {

    private volatile long lastTotalSuccessCount = 0;

    private volatile long lastExecutionErrorCount = 0;

    private ConcurrentHashMap<String, Long> lastSuccessCountMap = new ConcurrentHashMap<>();

    private final String[] messageTypes;

    /**
     * 构造一个异步消息消费者监控数据采集器
     */
    public AsyncMessageConsumerDataCollector() {
        this(new String[0]);
    }

    /**
     * 构造一个异步消息消费者监控数据采集器，并会额外上报指定消息类型的成功消费次数
     *
     * @param messageTypes 需额外上报的消息类型，以 "," 进行分割
     */
    public AsyncMessageConsumerDataCollector(String messageTypes) {
        this(messageTypes.split(","));
    }

    /**
     * 构造一个异步消息消费者监控数据采集器，并会额外上报指定消息类型的成功消费次数
     *
     * @param messageTypes 需额外上报的消息类型数组
     */
    public AsyncMessageConsumerDataCollector(String[] messageTypes) {
        this.messageTypes = messageTypes;
    }

    @Override
    protected String getModuleName() {
        return "naiveasync";
    }

    @Override
    protected String getCollectorName() {
        return "consumer";
    }

    @Override
    public int getPeriod() {
        return 15;
    }

    @Override
    public List<FalconData> getList() {
        AsyncMessageConsumerMonitor monitor = AsyncMessageConsumerMonitorFactory.get();

        List<FalconData> falconDataList = new ArrayList<>();

        long totalSuccessCount = monitor.getTotalSuccessCount();
        falconDataList.add(create("_success", totalSuccessCount - lastTotalSuccessCount));
        lastTotalSuccessCount = totalSuccessCount;

        long executionErrorCount = monitor.getExecutionErrorCount();
        falconDataList.add(create("_exec_error", executionErrorCount - lastExecutionErrorCount));
        lastExecutionErrorCount = executionErrorCount;

        if (messageTypes != null && messageTypes.length > 0) {
            for (String messageType : messageTypes) {
                long successCount = monitor.getSuccessCount(messageType);
                long lastSuccessCount = lastSuccessCountMap.containsKey(messageType) ? lastSuccessCountMap.get(messageType) : 0;
                falconDataList.add(create("_" + messageType + "_success", successCount - lastSuccessCount));
                lastSuccessCountMap.put(messageType, successCount);
            }
        }

        return falconDataList;
    }
}
