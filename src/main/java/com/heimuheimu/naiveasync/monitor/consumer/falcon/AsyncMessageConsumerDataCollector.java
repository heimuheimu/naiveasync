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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 异步消息消费者监控数据采集器。
 *
 * @author heimuheimu
 */
public class AsyncMessageConsumerDataCollector extends AbstractFalconDataCollector {

    private volatile long lastTotalPolledCount = 0;

    private volatile long lastTotalDelayedMills = 0;

    private final ConcurrentHashMap<String, Long> lastPolledCountMap = new ConcurrentHashMap<>();

    private final ConcurrentHashMap<String, Long> lastDelayedMillsMap = new ConcurrentHashMap<>();

    private volatile long lastTotalSuccessCount = 0;

    private volatile long lastExecutionErrorCount = 0;

    private final ConcurrentHashMap<String, Long> lastSuccessCountMap = new ConcurrentHashMap<>();

    private final Map<String, String> messageTypeMap;

    /**
     * 构造一个异步消息消费者监控数据采集器。
     */
    public AsyncMessageConsumerDataCollector() {
        this(null);
    }

    /**
     * 构造一个异步消息消费者监控数据采集器，并会额外上报指定消息类型的监控数据。
     *
     * @param messageTypeMap 需额外上报的消息类型 Map，Key 为消息类型，Value 为该消息类型对应的 Metric 名称。
     */
    public AsyncMessageConsumerDataCollector(Map<String, String> messageTypeMap) {
        if (messageTypeMap == null) {
            messageTypeMap = new HashMap<>();
        }
        this.messageTypeMap = messageTypeMap;
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
        return 30;
    }

    @Override
    public List<FalconData> getList() {
        AsyncMessageConsumerMonitor monitor = AsyncMessageConsumerMonitorFactory.get();

        List<FalconData> falconDataList = new ArrayList<>();

        long totalPolledCount = monitor.getTotalPolledCount();
        long polledCount = totalPolledCount - lastTotalPolledCount;
        falconDataList.add(create("_polled", polledCount));
        lastTotalPolledCount = totalPolledCount;

        long totalDelayedMills = monitor.getTotalDelayedMills();
        long delayedMills = totalDelayedMills - lastTotalDelayedMills;
        double avgDelayedMills = polledCount == 0 ? 0 : (double) delayedMills / polledCount;
        falconDataList.add(create("_avg_delay", avgDelayedMills));
        lastTotalDelayedMills = totalDelayedMills;

        falconDataList.add(create("_max_delay", monitor.getMaxDelayedMills()));
        monitor.resetMaxDelayedMills();

        long totalSuccessCount = monitor.getTotalSuccessCount();
        falconDataList.add(create("_success", totalSuccessCount - lastTotalSuccessCount));
        lastTotalSuccessCount = totalSuccessCount;

        long executionErrorCount = monitor.getExecutionErrorCount();
        falconDataList.add(create("_exec_error", executionErrorCount - lastExecutionErrorCount));
        lastExecutionErrorCount = executionErrorCount;

        for (String messageType : messageTypeMap.keySet()) {
            String messageTypeMetric = messageTypeMap.get(messageType);

            long totalSpecificPolledCount = monitor.getPolledCount(messageType);
            long lastSpecificPolledCount = lastPolledCountMap.containsKey(messageType) ? lastPolledCountMap.get(messageType) : 0;
            long specificPolledCount = totalSpecificPolledCount - lastSpecificPolledCount;
            falconDataList.add(create("_" + messageTypeMetric + "_polled", specificPolledCount));
            lastPolledCountMap.put(messageType, totalSpecificPolledCount);

            long totalSpecificDelayedMills = monitor.getDelayedMills(messageType);
            long lastSpecificDelayedMills = lastDelayedMillsMap.containsKey(messageType) ? lastDelayedMillsMap.get(messageType) : 0;
            long specificDelayedMills = totalSpecificDelayedMills - lastSpecificDelayedMills;
            double avgSpecificDelayedMills = specificPolledCount == 0 ? 0 : (double) specificDelayedMills / specificPolledCount;
            falconDataList.add(create("_" + messageTypeMetric + "_avg_delay", specificPolledCount));
            lastDelayedMillsMap.put(messageType, totalSpecificDelayedMills);

            falconDataList.add(create("_" + messageTypeMetric + "_max_delay", monitor.getSpecificMaxDelayedMills(messageType)));
            monitor.resetSpecificMaxDelayedMills(messageType);

            long successCount = monitor.getSuccessCount(messageType);
            long lastSuccessCount = lastSuccessCountMap.containsKey(messageType) ? lastSuccessCountMap.get(messageType) : 0;
            falconDataList.add(create("_" + messageTypeMetric + "_success", successCount - lastSuccessCount));
            lastSuccessCountMap.put(messageType, successCount);
        }

        return falconDataList;
    }
}
