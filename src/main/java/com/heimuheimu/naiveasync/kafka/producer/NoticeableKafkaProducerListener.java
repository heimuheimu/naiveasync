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

package com.heimuheimu.naiveasync.kafka.producer;

import com.heimuheimu.naivemonitor.alarm.NaiveServiceAlarm;
import com.heimuheimu.naivemonitor.alarm.ServiceAlarmMessageNotifier;
import com.heimuheimu.naivemonitor.alarm.ServiceContext;
import com.heimuheimu.naivemonitor.util.MonitorUtil;

import java.util.List;
import java.util.Map;

/**
 * {@code NoticeableKafkaProducerListener} 监听 {@link KafkaProducer} 中的消息发送失败事件，通过报警消息通知器进行实时通知。
 *
 * <p><strong>说明：</strong>{@code NoticeableKafkaProducerListener} 类是线程安全的，可在多个线程中使用同一个实例。</p>
 *
 * @see NaiveServiceAlarm
 * @author heimuheimu
 */
public class NoticeableKafkaProducerListener extends KafkaProducerListenerSkeleton {

    /**
     * 使用 {@code KafkaProducer} 的项目名称
     */
    private final String project;

    /**
     * 使用 {@code KafkaProducer} 的主机名称
     */
    private final String host;

    /**
     * 服务不可用报警器
     */
    private final NaiveServiceAlarm naiveServiceAlarm;

    /**
     * 构造一个对消息发送失败事件进行实时通知的 {@link KafkaProducer} 事件监听器。
     *
     * @param project 使用 {@code KafkaProducer} 的项目名称
     * @param notifierList 使用 {@code KafkaProducer} 的项目名称
     * @throws IllegalArgumentException 如果报警消息通知器列表为 {@code null} 或空时，抛出此异常
     */
    public NoticeableKafkaProducerListener(String project, List<ServiceAlarmMessageNotifier> notifierList) {
        this(project, notifierList, null);
    }

    /**
     * 构造一个对消息发送失败事件进行实时通知的 {@link KafkaProducer} 事件监听器。
     *
     * @param project 使用 {@code KafkaProducer} 的项目名称
     * @param notifierList 使用 {@code KafkaProducer} 的项目名称
     * @param hostAliasMap 别名 Map，Key 为机器名， Value 为别名，允许为 {@code null}
     * @throws IllegalArgumentException 如果报警消息通知器列表为 {@code null} 或空时，抛出此异常
     */
    public NoticeableKafkaProducerListener(String project, List<ServiceAlarmMessageNotifier> notifierList,
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
    public void onErrorSent(String topicName, String bootstrapServers) {
        naiveServiceAlarm.onCrashed(getServiceContext(topicName, bootstrapServers));
    }

    /**
     * 根据发送失败的消息所在 Topic 名称和 Kafka 集群启动地址构造一个服务及服务所在的运行环境信息。
     *
     * @param topicName 发送失败的消息所在 Topic 名称
     * @param bootstrapServers Kafka 集群启动地址
     * @return 服务及服务所在的运行环境信息
     */
    protected ServiceContext getServiceContext(String topicName, String bootstrapServers) {
        ServiceContext serviceContext = new ServiceContext();
        serviceContext.setName("[ErrorSent][KafkaProducer] " + topicName);
        serviceContext.setHost(host);
        serviceContext.setProject(project);
        serviceContext.setRemoteHost(bootstrapServers);
        return serviceContext;
    }

    @Override
    public String toString() {
        return "NoticeableKafkaProducerListener{" +
                "project='" + project + '\'' +
                ", host='" + host + '\'' +
                '}';
    }
}
