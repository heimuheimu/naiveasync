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

package com.heimuheimu.async.kafka.producer;

import com.heimuheimu.async.kafka.KafkaUtil;
import com.heimuheimu.async.monitor.producer.AsyncMessageProducerMonitor;
import com.heimuheimu.async.monitor.producer.AsyncMessageProducerMonitorFactory;
import com.heimuheimu.async.producer.AsyncMessageProducer;
import com.heimuheimu.async.transcoder.MessageTranscoder;
import com.heimuheimu.async.transcoder.SimpleMessageTranscoder;
import org.apache.kafka.clients.producer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;

/**
 * 基于 kafka 框架实现的异步消息生产者，更多 kafka 信息请参考：<a href="http://kafka.apache.org/documentation/">http://kafka.apache.org/documentation/</a>
 *
 * @author heimuheimu
 */
public class KafkaAsyncMessageProducer implements AsyncMessageProducer, Closeable {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaAsyncMessageProducer.class);

    private final MessageTranscoder transcoder;

    private final Producer<byte[], byte[]> producer;

    private final AsyncMessageProducerMonitor monitor;

    public KafkaAsyncMessageProducer(KafkaProducerConfig kafkaProducerConfig) {
        transcoder = new SimpleMessageTranscoder();
        producer = new KafkaProducer<>(kafkaProducerConfig.toConfigMap());
        monitor = AsyncMessageProducerMonitorFactory.get();
    }

    @Override
    public <T> void send(T message) {
        if (message != null) {
            try {
                String topicName = KafkaUtil.getTopicName(message.getClass());
                byte[] value = transcoder.encode(message);
                ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(topicName, value);
                producer.send(record, new MessageSendCallback(topicName, monitor));
            } catch (Exception e) {
                monitor.onErrorSent(KafkaUtil.getTopicName(message.getClass()));
                LOGGER.error("Send async message failed: `" + e.getMessage() + "`. Message: `" + message + "`.", e);
            }
        }
    }

    @Override
    public void close() {
        producer.flush();
        producer.close();
    }

    private static class MessageSendCallback implements Callback {

        private final String topicName;

        private final AsyncMessageProducerMonitor monitor;

        public MessageSendCallback(String topicName, AsyncMessageProducerMonitor monitor) {
            this.topicName = topicName;
            this.monitor = monitor;
        }

        @Override
        public void onCompletion(RecordMetadata metadata, Exception exception) {
            if (exception == null) { //发送成功
                monitor.onSuccessSent(topicName);
            } else { //发送失败
                monitor.onErrorSent(topicName);
                LOGGER.error("Send async message failed: `" + exception.getMessage() + "`. Topic: `" + topicName + "`.", exception);
            }
        }

    }

}
