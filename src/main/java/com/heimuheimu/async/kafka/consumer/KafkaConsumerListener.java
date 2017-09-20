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

package com.heimuheimu.async.kafka.consumer;

import java.util.List;

/**
 * Kafka 消费者事件监听器
 *
 * @author heimuheimu
 */
public interface KafkaConsumerListener {

    /**
     * 当拉取新消息失败时，将触发此事件
     *
     * @param subscribedTopics 已订阅的 topic 名称列表
     */
    void onPollFailed(List<String> subscribedTopics);

    /**
     * 当拉取新消息失败后，又重新恢复，将触发此事件
     *
     * @param subscribedTopics 已订阅的 topic 名称列表
     */
    void onPollRecovered(List<String> subscribedTopics);

    /**
     * 当消息反序列化失败时，将触发此事件
     *
     * @param topic 消息所在 topic 名称
     */
    void onDecodeMessageFailed(String topic);

    /**
     * 当消息消费操作发生异常时，将触发此事件
     * <p>注意：消费者不应抛出任何异常，此事件被触发时通常意味着程序 BUG，消费操作定义：{@link com.heimuheimu.async.consumer.AsyncMessageConsumer#consume(List)}</p>
     *
     * @param topic 消息所在 topic 名称
     */
    void onConsumeFailed(String topic);

    /**
     * 无法为该类型消息找到对应的消费者时，将触发此事件
     *
     * @param topic 消息所在 topic 名称
     */
    void onUnrecognizedMessage(String topic);

    /**
     * 当 Kafka 断点提交失败时，将触发此事件
     *
     * @param subscribedTopics 已订阅的 topic 名称列表
     */
    void onCommitSyncFailed(List<String> subscribedTopics);

    /**
     * 当 Kafka 断点提交失败后，又重新恢复，将触发此事件
     *
     * @param subscribedTopics 已订阅的 topic 名称列表
     */
    void onCommitSyncRecovered(List<String> subscribedTopics);
}
