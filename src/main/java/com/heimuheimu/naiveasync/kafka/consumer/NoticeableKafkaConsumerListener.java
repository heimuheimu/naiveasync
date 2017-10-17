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

import com.heimuheimu.naivemonitor.MonitorUtil;
import com.heimuheimu.naivemonitor.alarm.NaiveServiceAlarm;
import com.heimuheimu.naivemonitor.alarm.ServiceAlarmMessageNotifier;
import com.heimuheimu.naivemonitor.alarm.ServiceContext;
import org.apache.kafka.common.TopicPartition;

import java.util.List;
import java.util.Map;

/**
 * {@code NoticeableKafkaConsumerListener} 监听 {@link KafkaConsumerManager} 在消费过程中出现的异常事件，通过报警消息通知器进行实时通知。
 *
 * <p><strong>说明：</strong>{@code NoticeableKafkaConsumerListener} 类是线程安全的，可在多个线程中使用同一个实例。</p>
 *
 * @author heimuheimu
 * @see NaiveServiceAlarm
 */
public class NoticeableKafkaConsumerListener extends KafkaConsumerListenerSkeleton {

    /**
     * 使用 {@code KafkaConsumerManager} 的项目名称
     */
    private final String project;

    /**
     * 使用 {@code KafkaConsumerManager} 的主机名称
     */
    private final String host;

    /**
     * 服务不可用报警器
     */
    private final NaiveServiceAlarm naiveServiceAlarm;

    /**
     * 构造一个对消费过程中出现的异常事件进行实时通知的 Kafka 消费者事件监听器。
     *
     * @param project 使用 {@code KafkaConsumerManager} 的项目名称
     * @param notifierList 使用 {@code KafkaConsumerManager} 的项目名称
     * @throws IllegalArgumentException 如果报警消息通知器列表为 {@code null} 或空时，抛出此异常
     */
    public NoticeableKafkaConsumerListener(String project, List<ServiceAlarmMessageNotifier> notifierList) {
        this(project, notifierList, null);
    }

    /**
     * 构造一个对消费过程中出现的异常事件进行实时通知的 Kafka 消费者事件监听器。
     *
     * @param project 使用 {@code KafkaConsumerManager} 的项目名称
     * @param notifierList 使用 {@code KafkaConsumerManager} 的项目名称
     * @param hostAliasMap 别名 Map，Key 为机器名， Value 为别名，允许为 {@code null}
     * @throws IllegalArgumentException 如果报警消息通知器列表为 {@code null} 或空时，抛出此异常
     */
    public NoticeableKafkaConsumerListener(String project, List<ServiceAlarmMessageNotifier> notifierList,
                                           Map<String, String> hostAliasMap) {
        this.project = project;
        this.naiveServiceAlarm = new NaiveServiceAlarm(notifierList);
        String host = MonitorUtil.getLocalHostName();
        if (hostAliasMap != null && hostAliasMap.containsKey(host)) {
            this.host = hostAliasMap.get(host);
        } else {
            this.host = host;
        }
    }

    @Override
    public void onPollFailed(String bootstrapServers) {
        ServiceContext serviceContext = getServiceContext(bootstrapServers);
        naiveServiceAlarm.onCrashed(serviceContext);
    }

    @Override
    public void onPollRecovered(String bootstrapServers) {
        ServiceContext serviceContext = getServiceContext(bootstrapServers);
        naiveServiceAlarm.onRecovered(serviceContext);
    }

    @Override
    public void onConsumeFailed(TopicPartition partition, Object message, String bootstrapServers) {
        ServiceContext serviceContext = getServiceContext(bootstrapServers);
        serviceContext.setName("[ErrorConsume][KafkaConsumer] " + partition.topic() + "-" + partition.partition());
        naiveServiceAlarm.onCrashed(serviceContext);
    }

    @Override
    public void onCommitSyncFailed(TopicPartition partition, String bootstrapServers) {
        ServiceContext serviceContext = getServiceContext(bootstrapServers);
        serviceContext.setName("[CommitSync][KafkaConsumer] " + partition.topic() + "-" + partition.partition());
        naiveServiceAlarm.onCrashed(serviceContext);
    }

    @Override
    public void onCommitSyncRecovered(TopicPartition partition, String bootstrapServers) {
        ServiceContext serviceContext = getServiceContext(bootstrapServers);
        serviceContext.setName("[CommitSync][KafkaConsumer] " + partition.topic() + "-" + partition.partition());
        naiveServiceAlarm.onRecovered(serviceContext);
    }

    @Override
    public void onPartitionPaused(TopicPartition partition, String bootstrapServers) {
        ServiceContext serviceContext = getServiceContext(bootstrapServers);
        serviceContext.setName("[PartitionPaused][KafkaConsumer] " + partition.topic() + "-" + partition.partition());
        naiveServiceAlarm.onCrashed(serviceContext);
    }

    protected ServiceContext getServiceContext(String bootstrapServers) {
        ServiceContext serviceContext = new ServiceContext();
        serviceContext.setName("KafkaConsumer");
        serviceContext.setHost(host);
        serviceContext.setProject(project);
        serviceContext.setRemoteHost(bootstrapServers);
        return serviceContext;
    }

    @Override
    public String toString() {
        return "NoticeableKafkaConsumerListener{" +
                "project='" + project + '\'' +
                ", host='" + host + '\'' +
                '}';
    }
}
