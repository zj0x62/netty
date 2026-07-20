/*
 * Copyright 2012 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package io.netty.channel;

import io.netty.util.concurrent.EventExecutorGroup;

/**
 * 特殊的 {@link EventExecutorGroup}，允许注册 {@link Channel}，注册后的 Channel 将在事件循环中
 * 被处理和选择。
 * <p>
 * 每个 {@link Channel} 在创建后都会注册到一个 {@link EventLoop}，该 EventLoop 负责处理该 Channel
 * 的所有 I/O 事件。一个 EventLoopGroup 包含多个 EventLoop，Channel 会被均匀分配到各个 EventLoop 上。
 * <p>
 * 典型用法：
 * <pre>
 * // bossGroup 负责接受连接，workerGroup 负责处理已建立连接的 I/O
 * EventLoopGroup bossGroup = new NioEventLoopGroup(1);
 * EventLoopGroup workerGroup = new NioEventLoopGroup();
 * ServerBootstrap b = new ServerBootstrap();
 * b.group(bossGroup, workerGroup)
 *  .channel(NioServerSocketChannel.class)
 *  .childHandler(new MyChannelInitializer());
 * </pre>
 */
public interface EventLoopGroup extends EventExecutorGroup {
    /**
     * 返回下一个待使用的 {@link EventLoop}。
     * <p>
     * 通常采用轮询（Round-Robin）策略在 EventLoopGroup 中的 EventLoop 之间分配 Channel。
     *
     * @return 下一个可用的 EventLoop
     */
    @Override
    EventLoop next();

    /**
     * 将指定的 {@link Channel} 注册到该 EventLoopGroup。返回的 {@link ChannelFuture}
     * 将在注册完成后收到通知。
     *
     * @param channel 要注册的 Channel
     * @return 表示注册操作结果的 ChannelFuture
     */
    ChannelFuture register(Channel channel);

    /**
     * 使用指定的 {@link ChannelPromise} 将 {@link Channel} 注册到该 EventLoopGroup。
     * 传入的 ChannelPromise 将在注册完成后收到通知并作为返回值返回。
     *
     * @param promise 用于接收注册结果的 ChannelPromise
     * @return 表示注册操作结果的 ChannelFuture
     */
    ChannelFuture register(ChannelPromise promise);

    /**
     * 使用指定的 {@link ChannelPromise} 将 {@link Channel} 注册到该 EventLoopGroup。
     * 传入的 ChannelPromise 将在注册完成后收到通知并作为返回值返回。
     *
     * @param channel 要注册的 Channel
     * @param promise 用于接收注册结果的 ChannelPromise
     * @return 表示注册操作结果的 ChannelFuture
     * @deprecated 请使用 {@link #register(ChannelPromise)} 代替
     */
    @Deprecated
    ChannelFuture register(Channel channel, ChannelPromise promise);
}
