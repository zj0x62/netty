/*
 * Copyright 2013 The Netty Project
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
package io.netty.util.concurrent;

import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;


/**
 * 异步操作的结果。
 * <p>
 * 该接口扩展了 JDK 的 {@link java.util.concurrent.Future}，提供了更丰富的异步操作支持，
 * 包括监听器机制、同步等待、异常获取等功能。它是 Netty 异步编程模型的基石，
 * {@link io.netty.channel.ChannelFuture} 等接口均继承自该接口。
 *
 * @param <V> 异步操作结果的类型
 */
@SuppressWarnings("ClassNameSameAsAncestorName")
public interface Future<V> extends java.util.concurrent.Future<V> {

    /**
     * 当且仅当 I/O 操作成功完成时返回 {@code true}。
     *
     * @return 如果操作成功完成返回 {@code true}，否则返回 {@code false}
     */
    boolean isSuccess();

    /**
     * 当且仅当该操作可通过 {@link #cancel(boolean)} 取消时返回 {@code true}。
     *
     * @return 如果操作可取消返回 {@code true}，否则返回 {@code false}
     */
    boolean isCancellable();

    /**
     * 返回 I/O 操作失败的原因。
     *
     * @return 失败原因；如果操作成功或该 Future 尚未完成则返回 {@code null}
     */
    Throwable cause();

    /**
     * 向该 Future 添加指定的监听器。当该 Future {@linkplain #isDone() 完成}时，指定的监听器将被通知。
     * 如果该 Future 已经完成，指定的监听器将立即被通知。
     *
     * @param listener 要添加的监听器
     * @return 该 Future 自身，支持链式调用
     */
    Future<V> addListener(GenericFutureListener<? extends Future<? super V>> listener);

    /**
     * 向该 Future 添加多个监听器。当该 Future {@linkplain #isDone() 完成}时，所有指定的监听器将被通知。
     * 如果该 Future 已经完成，所有指定的监听器将立即被通知。
     *
     * @param listeners 要添加的监听器数组
     * @return 该 Future 自身，支持链式调用
     */
    Future<V> addListeners(GenericFutureListener<? extends Future<? super V>>... listeners);

    /**
     * 从该 Future 中移除指定监听器的第一个匹配项。被移除的监听器将不再在该 Future
     * {@linkplain #isDone() 完成}时收到通知。如果指定的监听器未关联到该 Future，该方法不执行任何操作。
     *
     * @param listener 要移除的监听器
     * @return 该 Future 自身，支持链式调用
     */
    Future<V> removeListener(GenericFutureListener<? extends Future<? super V>> listener);

    /**
     * 从该 Future 中移除每个指定监听器的第一个匹配项。被移除的监听器将不再在该 Future
     * {@linkplain #isDone() 完成}时收到通知。如果指定的监听器未关联到该 Future，该方法不执行任何操作。
     *
     * @param listeners 要移除的监听器数组
     * @return 该 Future 自身，支持链式调用
     */
    Future<V> removeListeners(GenericFutureListener<? extends Future<? super V>>... listeners);

    /**
     * 等待该 Future 直到完成，如果操作失败则重新抛出失败原因。
     * <p>
     * 该方法会响应中断，如果等待过程中线程被中断将抛出 {@link InterruptedException}。
     *
     * @return 该 Future 自身，支持链式调用
     * @throws InterruptedException 如果当前线程在等待时被中断
     */
    Future<V> sync() throws InterruptedException;

    /**
     * 等待该 Future 直到完成（不可中断），如果操作失败则重新抛出失败原因。
     * <p>
     * 该方法不会响应中断，即使线程被中断也会继续等待。
     *
     * @return 该 Future 自身，支持链式调用
     */
    Future<V> syncUninterruptibly();

    /**
     * 等待该 Future 直到完成。
     * <p>
     * 该方法会响应中断，如果等待过程中线程被中断将抛出 {@link InterruptedException}。
     *
     * @return 该 Future 自身，支持链式调用
     * @throws InterruptedException 如果当前线程在等待时被中断
     */
    Future<V> await() throws InterruptedException;

    /**
     * 等待该 Future 直到完成（不可中断）。
     * <p>
     * 该方法捕获 {@link InterruptedException} 并静默丢弃，不会响应中断。
     *
     * @return 该 Future 自身，支持链式调用
     */
    Future<V> awaitUninterruptibly();

    /**
     * 在指定时间范围内等待该 Future 完成。
     *
     * @param timeout 超时时间
     * @param unit    超时时间单位
     * @return 当且仅当该 Future 在指定时间范围内完成时返回 {@code true}
     * @throws InterruptedException 如果当前线程在等待时被中断
     */
    boolean await(long timeout, TimeUnit unit) throws InterruptedException;

    /**
     * 在指定毫秒数内等待该 Future 完成。
     *
     * @param timeoutMillis 超时时间（毫秒）
     * @return 当且仅当该 Future 在指定时间范围内完成时返回 {@code true}
     * @throws InterruptedException 如果当前线程在等待时被中断
     */
    boolean await(long timeoutMillis) throws InterruptedException;

    /**
     * 在指定时间范围内等待该 Future 完成（不可中断）。
     * <p>
     * 该方法捕获 {@link InterruptedException} 并静默丢弃，不会响应中断。
     *
     * @param timeout 超时时间
     * @param unit    超时时间单位
     * @return 当且仅当该 Future 在指定时间范围内完成时返回 {@code true}
     */
    boolean awaitUninterruptibly(long timeout, TimeUnit unit);

    /**
     * 在指定毫秒数内等待该 Future 完成（不可中断）。
     * <p>
     * 该方法捕获 {@link InterruptedException} 并静默丢弃，不会响应中断。
     *
     * @param timeoutMillis 超时时间（毫秒）
     * @return 当且仅当该 Future 在指定时间范围内完成时返回 {@code true}
     */
    boolean awaitUninterruptibly(long timeoutMillis);

    /**
     * 非阻塞地获取异步操作的结果。如果该 Future 尚未完成，将返回 {@code null}。
     * <p>
     * 由于 {@code null} 本身可能是操作成功的结果值，因此不能仅依赖返回值来判断操作是否完成，
     * 还需要结合 {@link #isDone()} 进行确认。
     *
     * @return 异步操作的结果，如果未完成则返回 {@code null}
     */
    V getNow();

    /**
     * {@inheritDoc}
     * <p>
     * 如果取消成功，将以 {@link CancellationException} 作为失败原因来标记该 Future 为失败状态。
     *
     * @param mayInterruptIfRunning 是否中断正在执行操作的线程
     * @return 如果该 Future 被成功取消返回 {@code true}
     */
    @Override
    boolean cancel(boolean mayInterruptIfRunning);
}
