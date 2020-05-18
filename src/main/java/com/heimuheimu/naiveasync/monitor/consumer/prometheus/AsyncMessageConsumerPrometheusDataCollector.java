/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2020 heimuheimu
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

package com.heimuheimu.naiveasync.monitor.consumer.prometheus;

import com.heimuheimu.naiveasync.monitor.consumer.AsyncMessageConsumerMonitor;
import com.heimuheimu.naiveasync.monitor.consumer.AsyncMessageConsumerMonitorFactory;
import com.heimuheimu.naivemonitor.prometheus.PrometheusCollector;
import com.heimuheimu.naivemonitor.prometheus.PrometheusData;
import com.heimuheimu.naivemonitor.prometheus.PrometheusSample;
import com.heimuheimu.naivemonitor.util.DeltaCalculator;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * 异步消息消费者监控信息采集器。采集时会返回以下数据：
 * <ul>
 *     <li>naiveasync_consumer_polled_count{type="$messageType"} 相邻两次采集周期内已拉取的消息总数</li>
 *     <li>naiveasync_consumer_success_count{type="$messageType"} 相邻两次采集周期内已消费成功的消息总数</li>
 *     <li>naiveasync_consumer_max_delay_milliseconds{type="$messageType"} 相邻两次采集周期内消息到达最大延迟时间（消息延迟时间 = 消息拉取时间 - 消息发送时间），单位：毫秒</li>
 *     <li>naiveasync_consumer_avg_delay_milliseconds{type="$messageType"} 相邻两次采集周期内消息到达平均延迟时间（消息延迟时间 = 消息拉取时间 - 消息发送时间），单位：毫秒</li>
 *     <li>naiveasync_consumer_exec_error_count 相邻两次采集周期内消费出错次数，包含 Kafka 操作出现的错误和消费过程中出现的错误，不区分消息类型</li>
 * </ul>
 *
 * @author heimuheimu
 * @since 1.2
 */
public class AsyncMessageConsumerPrometheusDataCollector implements PrometheusCollector {

    /**
     * 差值计算器
     */
    private final DeltaCalculator deltaCalculator = new DeltaCalculator();

    @Override
    public List<PrometheusData> getList() {
        AsyncMessageConsumerMonitor monitor = AsyncMessageConsumerMonitorFactory.get();
        Set<String> messageTypes = monitor.getMessageTypeSet();
        List<PrometheusData> dataList = new ArrayList<>();
        if (!messageTypes.isEmpty()) {
            PrometheusData polledCountData = PrometheusData.buildGauge("naiveasync_consumer_polled_count", "");
            PrometheusData avgDelayData = PrometheusData.buildGauge("naiveasync_consumer_avg_delay_milliseconds", "");
            PrometheusData maxDelayData = PrometheusData.buildGauge("naiveasync_consumer_max_delay_milliseconds", "");
            PrometheusData successCountData = PrometheusData.buildGauge("naiveasync_consumer_success_count", "");
            for (String messageType : monitor.getMessageTypeSet()) {
                double polledCount = deltaCalculator.delta("polled_" + messageType, monitor.getPolledCount(messageType));
                double delayedMills = deltaCalculator.delta("delayed_" + messageType, monitor.getDelayedMills(messageType));
                double avgDelayedMills = polledCount > 0 ? delayedMills / polledCount : 0;
                polledCountData.addSample(PrometheusSample.build(polledCount).addSampleLabel("type", messageType));
                avgDelayData.addSample(PrometheusSample.build(avgDelayedMills).addSampleLabel("type", messageType));
                maxDelayData.addSample(PrometheusSample.build(monitor.getSpecificMaxDelayedMills(messageType))
                        .addSampleLabel("type", messageType));
                monitor.resetSpecificMaxDelayedMills(messageType);
                successCountData.addSample(PrometheusSample.build(deltaCalculator.delta("success_" + messageType, monitor.getSuccessCount(messageType)))
                        .addSampleLabel("type", messageType));
            }
            dataList.add(polledCountData);
            dataList.add(avgDelayData);
            dataList.add(maxDelayData);
            dataList.add(successCountData);
        }
        PrometheusData executionErrorCountData = PrometheusData.buildGauge("naiveasync_consumer_exec_error_count", "");
        executionErrorCountData.addSample(PrometheusSample.build(deltaCalculator.delta("ExecutionError", monitor.getExecutionErrorCount())));
        dataList.add(executionErrorCountData);
        return dataList;
    }
}
