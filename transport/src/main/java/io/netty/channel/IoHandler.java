/*
 * Copyright 2024 The Netty Project
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

import io.netty.util.concurrent.ThreadAwareExecutor;

/**
 * 为 {@link ThreadAwareExecutor} 处理 I/O 事件分发的处理器。
 * <p>
 * 除 {@link #wakeup()} 和 {@link #isCompatible(Class)} 夗，所有操作<strong>必须</strong>在
 * {@link ThreadAwareExecutor} 的线程上执行（即 {@link ThreadAwareExecutor#isExecutorThread(Thread)}
 * 必须返回 {@code true}），且不应由用户直接调用。
 * <p>
 * 一旦通过 {@link #register(IoHandle)} 方法注册了 {@link IoHandle}，就可以通过
 * {@link IoRegistration#submit(IoOps)} 提交与该 IoHandle 关联的 {@link IoOps}。
 * 这些提交的 IoOps 是 {@link IoEvent} 的"来源"，IoEvent 会通过
 * {@link IoHandle#handle(IoRegistration, IoEvent)} 方法分发给已注册的 IoHandle。
 * 这些事件必须被消费（处理），否则可能会被重复上报直到被处理。
 * <p>
 * 该接口是 Netty 4.2 中对 I/O 多路复用的抽象层，不同的实现（如 NIO Selector、Epoll、Kqueue、IO_Uring）
 * 通过不同的 IoHandler 实现来适配平台特定的 I/O 机制。
 */
public interface IoHandler {

    /**
     * 初始化该 {@link IoHandler}。
     * <p>
     * 在开始处理 I/O 事件之前调用，用于执行必要的初始化操作（如创建 Selector 等）。
     */
    default void initialize() { }

    /**
     * 执行该 {@link IoHandler} 负责的 I/O 事件处理。
     * <p>
     * 应使用 {@link IoHandlerContext} 来确保不会执行过长时间，从而阻塞 {@link ThreadAwareExecutor}
     * 上调度的其他任务。通过参考 {@link IoHandlerContext#delayNanos(long)} 或
     * {@link IoHandlerContext#deadlineNanos()} 来控制执行时长。
     *
     * @param context I/O 上下文，提供时间控制信息
     * @return 本次处理中已处理 I/O 事件的 {@link IoHandle} 数量。
     *         内部事件（如唤醒和定时器过期）不应计入此数量
     */
    int run(IoHandlerContext context);

    /**
     * 准备销毁该 {@link IoHandler}。该方法将在 {@link #destroy()} 之前被调用，且可能被多次调用。
     * <p>
     * 用于在正式销毁前执行清理准备工作，如停止接受新的注册等。
     */
    default void prepareToDestroy() { }

    /**
     * 销毁该 {@link IoHandler} 并释放其所有资源。一旦销毁，继续使用该 IoHandler 将导致未定义行为。
     */
    default void destroy() { }

    /**
     * 注册一个 {@link IoHandle} 以接收 I/O 事件。
     *
     * @param handle 要注册的 IoHandle
     * @return 与该 IoHandle 关联的注册对象，用于提交 I/O 操作
     * @throws Exception 注册过程中发生错误时抛出
     */
    IoRegistration register(IoHandle handle) throws Exception;

    /**
     * 唤醒该 {@link IoHandler}。如果有任何阻塞操作，应立即解除阻塞并尽快返回。
     * <p>
     * 该方法可以在任意线程上调用，用于唤醒正在阻塞等待 I/O 事件的 EventLoop 线程。
     */
    void wakeup();

    /**
     * 判断给定的 {@link IoHandle} 类型是否与该 {@link IoHandler} 兼容，即是否可以被注册。
     *
     * @param handleType IoHandle 的类型
     * @return 如果兼容返回 {@code true}，否则返回 {@code false}
     */
    boolean isCompatible(Class<? extends IoHandle> handleType);
}
