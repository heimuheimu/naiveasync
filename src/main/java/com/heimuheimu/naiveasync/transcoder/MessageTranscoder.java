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

package com.heimuheimu.naiveasync.transcoder;

/**
 * 异步消息转换器
 *
 * @author heimuheimu
 */
public interface MessageTranscoder {

    /**
     * 将 Java 对象编码成字节数组
     *
     * @param message Java 对象
     * @return 编码后的字节数组
     * @throws TranscoderException 如果编码过程中发生错误，则抛出此异常
     */
    byte[] encode(Object message) throws TranscoderException;

    /**
     * 将字节数组还原成 Java 对象并返回
     *
     * @param src 需要解码的字节数组
     * @return Java 对象
     * @throws TranscoderException 如果解码过程中发生错误，则抛出此异常
     */
    <T> T decode(byte[] src) throws TranscoderException;

}
