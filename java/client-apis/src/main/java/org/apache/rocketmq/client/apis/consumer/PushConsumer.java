/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.client.apis.consumer;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import org.apache.rocketmq.client.apis.ClientException;

/**
 * Push consumer is a thread-safe and fully-managed rocketmq client which is used to consume messages by the group.
 *
 * <p>Consumers belong to the same consumer group share messages from the server, which means they must have the same
 * subscription expressions, otherwise the behavior is <strong>undefined</strong>. If a new consumer group's consumer
 * is started for the first time, it consumes from the latest position. Once the consumer is started, the server
 * records its consumption progress and derives it in the subsequent startup, or we can call it clustering mode.
 *
 * <h3>Clustering mode</h3>
 * <pre>
 * ┌──────────────────┐        ┌──────────┐
 * │consume progress 0│◄─┐  ┌─►│consumer A│
 * └──────────────────┘  │  │  └──────────┘
 *                       ├──┤
 *  ┌─────────────────┐  │  │  ┌──────────┐
 *  │topic X + group 0│◄─┘  └─►│consumer B│
 *  └─────────────────┘        └──────────┘
 * </pre>
 *
 * <p>As for broadcasting mode, you may intend to maintain different consumption progress for different consumers,
 * different consumer groups should be set in this case.
 *
 * <h3>Broadcasting mode</h3>
 * <pre>
 * ┌──────────────────┐     ┌──────────┐     ┌──────────────────┐
 * │consume progress 0│◄─┬──┤consumer A│  ┌─►│consume progress 1│
 * └──────────────────┘  │  └──────────┘  │  └──────────────────┘
 *                       │                │
 *  ┌─────────────────┐  │  ┌──────────┐  │  ┌─────────────────┐
 *  │topic X + group 0│◄─┘  │consumer B├──┴─►│topic X + group 1│
 *  └─────────────────┘     └──────────┘     └─────────────────┘
 * </pre>
 *
 * <p>To accelerate the message consumption, push consumer applies
 * <a href="https://en.wikipedia.org/wiki/Reactive_Streams">reactive streams</a>
 * . Messages received from server is cached locally before consumption,
 * {@link PushConsumerBuilder#setMaxCacheMessageCount(int)} and
 * {@link PushConsumerBuilder#setMaxCacheMessageSizeInBytes(int)} could be used to set the cache threshold in
 * different dimension.
 */
public interface PushConsumer extends Closeable {
    /**
     * Get the load balancing group for the consumer.
     *
     * @return consumer load balancing group.
     */
    String getConsumerGroup();

    /**
     * List the existed subscription expressions in push consumer.
     *
     * @return collections of the subscription expression.
     */
    Map<String, FilterExpression> getSubscriptionExpressions();

    /**
     * Add subscription expression dynamically.
     *
     * @param filterExpression new filter expression to add.
     * @return push consumer instance.
     */
    PushConsumer subscribe(String topic, FilterExpression filterExpression) throws ClientException;

    /**
     * Remove subscription expression dynamically by topic.
     *
     * <p>It stops the backend task to fetch messages from the server, and besides that, the locally cached message
     * whose topic was removed before would not be delivered to {@link MessageListener} anymore.
     *
     * <p>Nothing occurs if the specified topic does not exist in subscription expressions of the push consumer.
     *
     * @param topic the topic to remove the subscription.
     * @return push consumer instance.
     */
    PushConsumer unsubscribe(String topic) throws ClientException;

    /**
     * Close the push consumer and release all related resources.
     *
     * <p>Once push consumer is closed, <strong>it could not be started once again.</strong> we maintained an FSM
     * (finite-state machine) to record the different states for each producer.
     */
    @Override
    void close() throws IOException;
}
