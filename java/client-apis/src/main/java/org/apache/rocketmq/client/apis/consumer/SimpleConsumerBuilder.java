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

import java.time.Duration;
import java.util.Map;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientException;

/**
 * Builder to config and start {@link SimpleConsumer}.
 */
public interface SimpleConsumerBuilder {
    /**
     * Set the client configuration for the simple consumer.
     *
     * @param clientConfiguration client's configuration.
     * @return the simple consumer builder instance.
     */
    SimpleConsumerBuilder setClientConfiguration(ClientConfiguration clientConfiguration);

    /**
     * Set the load balancing group for the simple consumer.
     *
     * @param consumerGroup consumer load balancing group.
     * @return the consumer builder instance.
     */
    SimpleConsumerBuilder setConsumerGroup(String consumerGroup);

    /**
     * Add subscription expressions for the simple consumer.
     *
     * @param subscriptionExpressions subscriptions to add which use the map of topics to filter expressions.
     * @return the consumer builder instance.
     */
    SimpleConsumerBuilder setSubscriptionExpressions(Map<String, FilterExpression> subscriptionExpressions);

    /**
     * Set the max await time when receive messages from the server.
     * The simple consumer will hold this long-polling receive requests until  a message is returned or a timeout
     * occurs.
     *
     * @param awaitDuration The maximum time to block when no message is available.
     * @return the consumer builder instance.
     */
    SimpleConsumerBuilder setAwaitDuration(Duration awaitDuration);

    /**
     * Finalize the build of the {@link SimpleConsumer} instance and start.
     *
     * <p>This method will block until the simple consumer starts successfully.
     *
     * @return the simple consumer instance.
     */
    SimpleConsumer build() throws ClientException;
}
