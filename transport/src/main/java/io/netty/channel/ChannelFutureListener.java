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

import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;


/**
 * 监听 {@link ChannelFuture} 的结果。当通过 {@link ChannelFuture#addListener(GenericFutureListener)}
 * 添加该监听器后，异步 {@link Channel} I/O 操作完成时将收到通知。
 *
 * <h3>尽快将控制权返回给调用者</h3>
 *
 * {@link #operationComplete(Future)} 由 I/O 线程直接调用。因此，在该方法中执行耗时任务或阻塞操作
 * 可能导致 I/O 处理出现意外暂停。如果需要在 I/O 完成后执行阻塞操作，请尝试使用线程池在其他线程中执行。
 *
 * <h3>典型应用场景</h3>
 * <ul>
 *   <li>操作完成后关闭 Channel</li>
 *   <li>操作失败时执行清理逻辑</li>
 *   <li>链式触发后续 I/O 操作</li>
 *   <li>将异常传播到 Pipeline</li>
 * </ul>
 */
public interface ChannelFutureListener extends GenericFutureListener<ChannelFuture> {

    /**
     * 关闭与指定 {@link ChannelFuture} 关联的 {@link Channel} 的监听器。
     * <p>
     * 无论操作成功还是失败，都会关闭 Channel。适用于操作完成后需要释放连接的场景。
     */
    ChannelFutureListener CLOSE = new ChannelFutureListener() {
        @Override
        public void operationComplete(ChannelFuture future) {
            future.channel().close();
        }
    };

    /**
     * 仅在操作失败或被取消时关闭 {@link Channel} 的监听器。
     * <p>
     * 如果操作成功则不执行任何操作。适用于"操作失败则断开连接"的容错场景。
     */
    ChannelFutureListener CLOSE_ON_FAILURE = new ChannelFutureListener() {
        @Override
        public void operationComplete(ChannelFuture future) {
            if (!future.isSuccess()) {
                future.channel().close();
            }
        }
    };

    /**
     * 在操作失败时将异常转发到 {@link ChannelPipeline} 的监听器。
     * <p>
     * 该监听器模拟了 Netty 3 的旧有行为，将 {@link ChannelFuture} 中的 {@link Throwable}
     * 通过 {@link ChannelPipeline#fireExceptionCaught(Throwable)} 传播，使 Pipeline 中的
     * 异常处理器能够捕获并处理该异常。
     */
    ChannelFutureListener FIRE_EXCEPTION_ON_FAILURE = new ChannelFutureListener() {
        @Override
        public void operationComplete(ChannelFuture future) {
            if (!future.isSuccess()) {
                future.channel().pipeline().fireExceptionCaught(future.cause());
            }
        }
    };
}
