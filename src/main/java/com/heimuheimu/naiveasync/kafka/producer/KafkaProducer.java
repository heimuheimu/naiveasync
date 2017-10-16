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

import com.heimuheimu.naiveasync.kafka.KafkaUtil;
import com.heimuheimu.naiveasync.monitor.producer.AsyncMessageProducerMonitor;
import com.heimuheimu.naiveasync.monitor.producer.AsyncMessageProducerMonitorFactory;
import com.heimuheimu.naiveasync.producer.AsyncMessageProducer;
import com.heimuheimu.naiveasync.transcoder.MessageTranscoder;
import com.heimuheimu.naiveasync.transcoder.SimpleMessageTranscoder;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;

/**
 * 基于 kafka 框架实现的异步消息生产者，更多 kafka 信息请参考：<a href="http://kafka.apache.org/documentation/">http://kafka.apache.org/documentation/</a>。
 *
 * <h3>监听器</h3>
 * <blockquote>
 * 当 {@code KafkaProducer} 发送消息失败时，会触发 {@link KafkaProducerListener} 相应的事件进行通知。
 * </blockquote>
 *
 * <p><strong>说明：</strong>{@code KafkaProducer} 类是线程安全的，可在多个线程中使用同一个实例。</p>
 *
 * @author heimuheimu
 */
public class KafkaProducer implements AsyncMessageProducer, Closeable {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProducer.class);

    /**
     * 消息与字节数组转换器
     */
    private final MessageTranscoder transcoder;

    /**
     * Kafka 消息生产者使用的配置信息
     */
    private final KafkaProducerConfig producerConfig;

    /**
     * Kafka 消息生产者
     */
    private final Producer<byte[], byte[]> producer;

    /**
     * {@code KafkaProducer} 事件监听器
     */
    private final KafkaProducerListener kafkaProducerListener;

    /**
     * 异步消息生产者信息监控器
     */
    private final AsyncMessageProducerMonitor monitor;

    /**
     * 构造一个 Kafka 异步消息生产者。
     *
     * @param kafkaProducerConfig Kafka 生产者配置信息，不允许为 {@code null}
     * @param kafkaProducerListener {@code KafkaProducer} 事件监听器，允许为 {@code null}
     */
    public KafkaProducer(KafkaProducerConfig kafkaProducerConfig, KafkaProducerListener kafkaProducerListener) throws NullPointerException {
        if (kafkaProducerConfig == null) {
            LOGGER.error("Create KafkaProducer failed: `kafkaProducerConfig could not be null`.");
            throw new NullPointerException("Create KafkaProducer failed: `kafkaProducerConfig could not be null`.");
        }
        this.transcoder = new SimpleMessageTranscoder();
        this.producerConfig = kafkaProducerConfig;
        this.producer = new org.apache.kafka.clients.producer.KafkaProducer<>(this.producerConfig.toConfigMap());
        this.kafkaProducerListener = kafkaProducerListener;
        this.monitor = AsyncMessageProducerMonitorFactory.get();
    }

    @Override
    public <T> void send(T message) {
        if (message != null) {
            String topicName = KafkaUtil.getTopicName(message.getClass());
            try {
                byte[] value = transcoder.encode(message);
                ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(topicName, value);
                producer.send(record, new MessageSendCallback(topicName));
            } catch (Exception e) {
                LOGGER.error("Send async message failed: `" + e.getMessage() + "`. Message: `" + message + "`. KafkaProducer: `"
                        + this + "`.", e);
                monitor.onErrorSent(topicName);
                if (kafkaProducerListener != null) {
                    try {
                        kafkaProducerListener.onErrorSent(topicName, producerConfig.getBootstrapServers());
                    } catch (Exception e1) {
                        LOGGER.error("Call KafkaProducerListener#onErrorSent() failed. Topic: `" + topicName
                                + "`. KafkaProducer: `" + this + "`.", e1);
                    }
                }
            }
        }
    }

    @Override
    public void close() {
        try {
            producer.flush();
            producer.close();
        } catch (Exception e) {
            LOGGER.error("Close KafkaProducer failed: `" + e.getMessage() + "`. KafkaProducer: `" + this + "`.", e);
        }
    }

    @Override
    public String toString() {
        return "KafkaProducer{" +
                "producerConfig=" + producerConfig +
                ", kafkaProducerListener=" + kafkaProducerListener +
                '}';
    }

    /**
     * 当 Kafka 消息发送成功或失败时，将会回调 {@link #onCompletion(RecordMetadata, Exception)} 方法。
     */
    private class MessageSendCallback implements Callback {

        private final String topicName;

        public MessageSendCallback(String topicName) {
            this.topicName = topicName;
        }

        @Override
        public void onCompletion(RecordMetadata metadata, Exception exception) {
            if (exception == null) { //发送成功
                monitor.onSuccessSent(topicName);
            } else { //发送失败
                LOGGER.error("Send async message failed: `" + exception.getMessage() + "`. Topic: `" + topicName
                        + "`. KafkaProducer: `" + this + "`.", exception);
                monitor.onErrorSent(topicName);
                if (kafkaProducerListener != null) {
                    try {
                        kafkaProducerListener.onErrorSent(topicName, producerConfig.getBootstrapServers());
                    } catch (Exception e1) {
                        LOGGER.error("Call KafkaProducerListener#onErrorSent() failed. Topic: `" + topicName
                                + "`. KafkaProducer: `" + this + "`.", e1);
                    }
                }
            }
        }

    }

}
