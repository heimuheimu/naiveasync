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

package com.heimuheimu.async.consumer;

import java.util.List;

/**
 * 异步消息消费者，仅支持同一类型的消息
 *
 * @author heimuheimu
 */
public interface AsyncMessageConsumer<T> {

    /**
     * 获得当前消费者支持的消息类型
     *
     * @return 获得当前消费者支持的消息类型
     */
    Class<T> getMessageClass();

    /**
     * 从 MQ 中消费异步消息列表，该方法不允许抛出异常
     * <p>注意：消费者需自行处理消费失败的情况，已消费过的消息列表不会被重复消费
     *
     * @param messageList 异步消息列表，不会为 {@code null}
     */
    void consume(List<T> messageList);

}
