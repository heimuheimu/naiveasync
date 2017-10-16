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

/**
 *  {@link KafkaProducer} 事件监听器，可监听 Kafka 消息发送失败事件。
 *
 *  <p>
 *     <strong>说明：</strong>监听器的实现类必须是线程安全的。应优先考虑继承 {@link KafkaProducerListenerSkeleton} 骨架类进行实现，
 *     防止 {@code KafkaProducerListener} 在后续版本增加监听事件时，带来的编译错误。
 * </p>
 *
 *  @author heimuheimu
 */
public interface KafkaProducerListener {

    /**
     * 当消息发送失败时，将触发该监听事件。
     *
     * @param topicName 消息在 Kafka 中对应的的 Topic 名称
     * @param bootstrapServers Kafka 集群启动地址，例如：host1:port1,host2:port2,...
     */
    void onErrorSent(String topicName, String bootstrapServers);
}
