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

import io.netty.bootstrap.Bootstrap;
import io.netty.util.concurrent.BlockingOperationException;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

import java.util.concurrent.TimeUnit;


/**
 * {@link Channel} I/O 操作的异步结果。
 * <p>
 * Netty 中所有的 I/O 操作都是异步的。这意味着任何 I/O 调用都会立即返回，
 * 但无法保证在调用结束时请求的 I/O 操作已经完成。取而代之的是，你会获得一个
 * {@link ChannelFuture} 实例，它提供了关于 I/O 操作结果或状态的信息。
 * <p>
 * {@link ChannelFuture} 只有两种状态：<em>未完成</em>和<em>已完成</em>。
 * 当一个 I/O 操作开始时，会创建一个新的 Future 对象。这个新的 Future 最初是未完成的——
 * 它既不是成功、失败，也不是取消，因为 I/O 操作尚未结束。如果 I/O 操作以成功、
 * 失败或取消的方式结束，Future 将被标记为已完成，并附带更具体的信息，例如失败原因。
 * 请注意，失败和取消也属于已完成状态。
 * <pre>
 *                                      +---------------------------+
 *                                      |      已完成（成功）        |
 *                                      +---------------------------+
 *                                 +---->      isDone() = true      |
 * +--------------------------+    |    |   isSuccess() = true      |
 * |        未完成             |    |    +===========================+
 * +--------------------------+    |    |      已完成（失败）        |
 * |      isDone() = false    |    |    +---------------------------+
 * |   isSuccess() = false    |----+---->      isDone() = true      |
 * | isCancelled() = false    |    |    |       cause() = 非null    |
 * |       cause() = null     |    |    +===========================+
 * +--------------------------+    |    |      已完成（取消）        |
 *                                 |    +---------------------------+
 *                                 +---->      isDone() = true      |
 *                                      | isCancelled() = true      |
 *                                      +---------------------------+
 * </pre>
 *
 * 提供了多种方法来检查 I/O 操作是否已完成、等待完成以及获取 I/O 操作的结果。
 * 还允许添加 {@link ChannelFutureListener}，以便在 I/O 操作完成时获得通知。
 *
 * <h3>优先使用 {@link #addListener(GenericFutureListener)} 而非 {@link #await()}</h3>
 *
 * 建议尽可能优先使用 {@link #addListener(GenericFutureListener)} 而非 {@link #await()}，
 * 以便在 I/O 操作完成时获得通知并执行后续任务。
 * <p>
 * {@link #addListener(GenericFutureListener)} 是非阻塞的。它只是将指定的
 * {@link ChannelFutureListener} 添加到 {@link ChannelFuture} 中，I/O 线程会在
 * 与该 Future 关联的 I/O 操作完成时通知监听器。{@link ChannelFutureListener} 提供了
 * 最佳的性能和资源利用率，因为它完全不阻塞，但如果你不习惯事件驱动编程，
 * 实现顺序逻辑可能会比较棘手。
 * <p>
 * 相比之下，{@link #await()} 是阻塞操作。一旦调用，调用线程将阻塞直到操作完成。
 * 使用 {@link #await()} 实现顺序逻辑更简单，但调用线程会不必要地阻塞直到 I/O 操作完成，
 * 而且线程间通知的开销相对较大。此外，在特定情况下可能会发生死锁，如下所述。
 *
 * <h3>不要在 {@link ChannelHandler} 内部调用 {@link #await()}</h3>
 * <p>
 * {@link ChannelHandler} 中的事件处理方法通常由 I/O 线程调用。如果在事件处理方法中
 * 调用 {@link #await()}，而该方法又是由 I/O 线程调用的，那么它等待的 I/O 操作
 * 可能永远不会完成，因为 {@link #await()} 会阻塞它正在等待的 I/O 操作，从而导致死锁。
 * <pre>
 * // 错误示范 - 永远不要这样做
 * {@code @Override}
 * public void channelRead({@link ChannelHandlerContext} ctx, Object msg) {
 *     {@link ChannelFuture} future = ctx.channel().close();
 *     future.awaitUninterruptibly();
 *     // 执行关闭后的操作
 *     // ...
 * }
 *
 * // 正确做法
 * {@code @Override}
 * public void channelRead({@link ChannelHandlerContext} ctx, Object msg) {
 *     {@link ChannelFuture} future = ctx.channel().close();
 *     future.addListener(new {@link ChannelFutureListener}() {
 *         public void operationComplete({@link ChannelFuture} future) {
 *             // 执行关闭后的操作
 *             // ...
 *         }
 *     });
 * }
 * </pre>
 * <p>
 * 尽管有上述缺点，但在某些情况下调用 {@link #await()} 确实更方便。在这种情况下，
 * 请确保不要在 I/O 线程中调用 {@link #await()}。否则，将抛出 {@link BlockingOperationException}
 * 以防止死锁。
 *
 * <h3>不要混淆 I/O 超时和 await 超时</h3>
 *
 * 通过 {@link #await(long)}、{@link #await(long, TimeUnit)}、{@link #awaitUninterruptibly(long)}
 * 或 {@link #awaitUninterruptibly(long, TimeUnit)} 指定的超时值与 I/O 超时完全无关。
 * 如果 I/O 操作超时，Future 将被标记为"已完成（失败）"，如上图所示。
 * 例如，连接超时应通过传输层特定的选项来配置：
 * <pre>
 * // 错误示范 - 永远不要这样做
 * {@link Bootstrap} b = ...;
 * {@link ChannelFuture} f = b.connect(...);
 * f.awaitUninterruptibly(10, TimeUnit.SECONDS);
 * if (f.isCancelled()) {
 *     // 连接尝试被用户取消
 * } else if (!f.isSuccess()) {
 *     // 这里可能会抛出 NullPointerException，因为 Future 可能尚未完成
 *     f.cause().printStackTrace();
 * } else {
 *     // 连接成功建立
 * }
 *
 * // 正确做法
 * {@link Bootstrap} b = ...;
 * // 配置连接超时选项
 * <b>b.option({@link ChannelOption}.CONNECT_TIMEOUT_MILLIS, 10000);</b>
 * {@link ChannelFuture} f = b.connect(...);
 * f.awaitUninterruptibly();
 *
 * // 现在可以确定 Future 已完成
 * assert f.isDone();
 *
 * if (f.isCancelled()) {
 *     // 连接尝试被用户取消
 * } else if (!f.isSuccess()) {
 *     f.cause().printStackTrace();
 * } else {
 *     // 连接成功建立
 * }
 * </pre>
 */
public interface ChannelFuture extends Future<Void> {

    /**
     * 返回与此 Future 关联的 I/O 操作所在的 {@link Channel}。
     *
     * @return 执行 I/O 操作的通道
     */
    Channel channel();

    @Override
    ChannelFuture addListener(GenericFutureListener<? extends Future<? super Void>> listener);

    @Override
    ChannelFuture addListeners(GenericFutureListener<? extends Future<? super Void>>... listeners);

    @Override
    ChannelFuture removeListener(GenericFutureListener<? extends Future<? super Void>> listener);

    @Override
    ChannelFuture removeListeners(GenericFutureListener<? extends Future<? super Void>>... listeners);

    @Override
    ChannelFuture sync() throws InterruptedException;

    @Override
    ChannelFuture syncUninterruptibly();

    @Override
    ChannelFuture await() throws InterruptedException;

    @Override
    ChannelFuture awaitUninterruptibly();

    /**
     * 判断此 {@link ChannelFuture} 是否为 Void Future（空操作的 Future）。
     * 如果返回 {@code true}，则不允许调用以下任何方法：
     * <ul>
     *     <li>{@link #addListener(GenericFutureListener)}</li>
     *     <li>{@link #addListeners(GenericFutureListener[])}</li>
     *     <li>{@link #await()}</li>
     *     <li>{@link #await(long, TimeUnit)}</li>
     *     <li>{@link #await(long)}</li>
     *     <li>{@link #awaitUninterruptibly()}</li>
     *     <li>{@link #sync()}</li>
     *     <li>{@link #syncUninterruptibly()}</li>
     * </ul>
     *
     * @return 如果是 Void Future 则返回 {@code true}，表示该 Future 不代表任何实际的 I/O 操作
     */
    boolean isVoid();
}
