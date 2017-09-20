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

import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka 生产者配置信息，更多内容可参考文档：<a href="http://kafka.apache.org/documentation/#producerconfigs">http://kafka.apache.org/documentation/#producerconfigs</a>
 *
 * @author heimuheimu
 */
public class KafkaProducerConfig {

    /**
     * Kafka 集群启动地址，例如：host1:port1,host2:port2,...
     */
    private String bootstrapServers = "";

    /**
     * 获得 Kafka 集群启动地址，例如：host1:port1,host2:port2,...
     *
     * @return Kafka 集群启动地址，例如：host1:port1,host2:port2,...
     */
    public String getBootstrapServers() {
        return bootstrapServers;
    }

    /**
     * 设置 Kafka 集群启动地址，例如：host1:port1,host2:port2,...
     *
     * @param bootstrapServers Kafka 集群启动地址，例如：host1:port1,host2:port2,...
     */
    public void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    /**
     * 根据当前配置信息返回一个用于构造 {@link org.apache.kafka.clients.producer.Producer} 实例的配置信息 Map
     *
     * @return Kafka 配置信息 Map
     */
    public Map<String, Object> toConfigMap() {
        HashMap<String, Object> configMap = new HashMap<>();
        configMap.put("bootstrap.servers", bootstrapServers);
        configMap.put("key.serializer", ByteArraySerializer.class);
        configMap.put("value.serializer", ByteArraySerializer.class);
        configMap.put("acks", "1"); // Leader 写入即返回
        configMap.put("buffer.memory", "33554432"); // 消息可缓存空间 32 MB
        configMap.put("compression.type", "none");
        configMap.put("max.block.ms", "60000"); //最大阻塞时间 60 秒
        return configMap;
    }

    @Override
    public String toString() {
        return "KafkaProducerConfig{" +
                "bootstrapServers='" + bootstrapServers + '\'' +
                '}';
    }
}
