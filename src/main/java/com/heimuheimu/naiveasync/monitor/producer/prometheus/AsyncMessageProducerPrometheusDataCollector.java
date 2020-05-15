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

package com.heimuheimu.naiveasync.monitor.producer.prometheus;

import com.heimuheimu.naiveasync.monitor.producer.AsyncMessageProducerMonitor;
import com.heimuheimu.naiveasync.monitor.producer.AsyncMessageProducerMonitorFactory;
import com.heimuheimu.naivemonitor.prometheus.PrometheusCollector;
import com.heimuheimu.naivemonitor.prometheus.PrometheusData;
import com.heimuheimu.naivemonitor.prometheus.PrometheusSample;
import com.heimuheimu.naivemonitor.util.DeltaCalculator;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * 异步消息生产者监控信息采集器，采集时会返回以下数据：
 * <ul>
 *     <li>naiveasync_producer_success_count{type="$messageType"} 相邻两次采集周期内发送成功的消息数</li>
 *     <li>naiveasync_producer_error_count{type="$messageType"} 相邻两次采集周期内发送失败的消息数</li>
 * </ul>
 *
 * @author heimuheimu
 * @since 1.2
 */
public class AsyncMessageProducerPrometheusDataCollector implements PrometheusCollector {

    /**
     * 差值计算器
     */
    private final DeltaCalculator deltaCalculator = new DeltaCalculator();

    @Override
    public List<PrometheusData> getList() {
        AsyncMessageProducerMonitor monitor = AsyncMessageProducerMonitorFactory.get();
        Set<String> messageTypeSet = monitor.getMessageTypeSet();
        List<PrometheusData> dataList = new ArrayList<>();
        if (!messageTypeSet.isEmpty()) {
            PrometheusData successCountData = PrometheusData.buildGauge("naiveasync_producer_success_count", "");
            PrometheusData errorCountData = PrometheusData.buildGauge("naiveasync_producer_error_count", "");
            for (String messageType : messageTypeSet) {
                successCountData.addSample(PrometheusSample.build(deltaCalculator.delta("success_" + messageType, monitor.getSuccessCount(messageType)))
                        .addSampleLabel("type", messageType));
                errorCountData.addSample(PrometheusSample.build(deltaCalculator.delta("error_" + messageType, monitor.getErrorCount(messageType)))
                        .addSampleLabel("type", messageType));
            }
        }
        return dataList;
    }
}
