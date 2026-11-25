/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.camel.kafkaconnector.utils;

import java.net.URI;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.camel.Consumer;
import org.apache.camel.Endpoint;
import org.apache.camel.Processor;
import org.apache.camel.Producer;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.DefaultComponent;
import org.apache.camel.support.DefaultEndpoint;
import org.apache.camel.support.DefaultExchange;
import org.apache.camel.util.URISupport;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SalesforcePubSubOffsetTest {
    private static final String URL = "salesforce:pubSubSubscribe:/event/Example__e?replayPreset=EARLIEST&pubSubDeserializeType=AVRO";

    @Test
    void createsStablePartitionAndRecordsReplayId() {
        SalesforcePubSubOffset offset = new SalesforcePubSubOffset(URL, "sandbox");
        SalesforcePubSubOffset same = new SalesforcePubSubOffset(
                "salesforce:pubSubSubscribe:/event/Example__e?replayPreset=LATEST", "sandbox");
        assertEquals(offset.partition(), same.partition());
        assertNotEquals(offset.partition(), new SalesforcePubSubOffset(URL, "production").partition());

        DefaultExchange exchange = new DefaultExchange(new DefaultCamelContext());
        exchange.getMessage().setHeader("CamelSalesforcePubSubReplayId", "a+/=");
        assertEquals(Map.of("replayId", "a+/="), offset.offset(exchange));
        exchange.getMessage().removeHeader("CamelSalesforcePubSubReplayId");
        assertThrows(ConnectException.class, () -> offset.offset(exchange));
        exchange.getMessage().setHeader("CamelSalesforcePubSubReplayId", 42);
        assertThrows(ConnectException.class, () -> offset.offset(exchange));
        exchange.getMessage().setHeader("CamelSalesforcePubSubReplayId", "invalid!");
        assertThrows(ConnectException.class, () -> offset.offset(exchange));
    }

    @Test
    void resumesWithEncodedReplayIdAndPreservesOptions() throws Exception {
        SalesforcePubSubOffset offset = new SalesforcePubSubOffset(URL, "sandbox");
        String resumed = offset.resumeUrl(reader(offset.partition(), Map.of("replayId", "a+/=")));
        Map<String, Object> parameters = URISupport.parseParameters(new URI(resumed));
        URISupport.resolveRawParameterValues(parameters);
        assertEquals("a+/=", parameters.get("pubSubReplayId"));
        assertEquals("CUSTOM", parameters.get("replayPreset"));
        assertEquals("AVRO", parameters.get("pubSubDeserializeType"));
        assertEquals("false", parameters.get("consumerWorkerPoolEnabled"));
        assertEquals("false", parameters.get("fallbackToLatestReplayId"));
        Map<String, Object> initial = URISupport.parseParameters(new URI(offset.resumeUrl(reader(offset.partition(), null))));
        assertEquals("EARLIEST", initial.get("replayPreset"));
        assertEquals("false", initial.get("consumerWorkerPoolEnabled"));
    }

    @Test
    void rejectsInvalidInputAndStoredOffsets() {
        assertThrows(ConnectException.class, () -> new SalesforcePubSubOffset("direct:start", "org"));
        assertThrows(ConnectException.class, () -> new SalesforcePubSubOffset(URL, " "));
        assertThrows(ConnectException.class, () -> new SalesforcePubSubOffset("salesforce:pubSubSubscribe:{{channel}}", "org"));
        SalesforcePubSubOffset offset = new SalesforcePubSubOffset(URL, "org");
        assertThrows(ConnectException.class, () -> offset.resumeUrl(reader(offset.partition(), Map.of("position", "old-id"))));
        assertThrows(ConnectException.class, () -> offset.resumeUrl(reader(offset.partition(), Map.of("replayId", 123))));
        assertThrows(ConnectException.class, () -> offset.resumeUrl(reader(offset.partition(), Map.of("replayId", "invalid!"))));
        SalesforcePubSubOffset customExecutor = new SalesforcePubSubOffset(URL + "&consumerWorkerPoolExecutorService=%23pool", "org");
        assertThrows(ConnectException.class, () -> customExecutor.resumeUrl(reader(customExecutor.partition(), null)));
    }

    @Test
    void camelComponentReceivesExactReplayId() throws Exception {
        SalesforcePubSubOffset offset = new SalesforcePubSubOffset(URL, "sandbox");
        String resumed = offset.resumeUrl(reader(offset.partition(), Map.of("replayId", "a+/=")));
        Map<String, Object> captured = new HashMap<>();
        try (DefaultCamelContext camel = new DefaultCamelContext()) {
            camel.addComponent("salesforce", new DefaultComponent() {
                @Override
                protected Endpoint createEndpoint(String uri, String remaining, Map<String, Object> parameters) {
                    captured.putAll(parameters);
                    parameters.clear();
                    return new DefaultEndpoint(uri, this) {
                        @Override
                        public Producer createProducer() {
                            throw new UnsupportedOperationException();
                        }

                        @Override
                        public Consumer createConsumer(Processor processor) {
                            throw new UnsupportedOperationException();
                        }
                    };
                }
            });
            camel.getEndpoint(resumed);
        }
        assertEquals("a+/=", captured.get("pubSubReplayId"));
        assertEquals("CUSTOM", captured.get("replayPreset"));
    }

    @Test
    void replacesCaseInsensitiveEndpointOptions() throws Exception {
        SalesforcePubSubOffset offset = new SalesforcePubSubOffset(
                "salesforce:pubSubSubscribe:/event/Example__e?replaypreset=LATEST&pubsubreplayid=old"
                        + "&consumerworkerpoolenabled=true&fallbacktolatestreplayid=true", "sandbox");
        Map<String, Object> parameters = URISupport.parseParameters(new URI(
                offset.resumeUrl(reader(offset.partition(), Map.of("replayId", "a+/=")))));
        assertFalse(parameters.containsKey("replaypreset"));
        assertFalse(parameters.containsKey("pubsubreplayid"));
        assertFalse(parameters.containsKey("consumerworkerpoolenabled"));
        assertFalse(parameters.containsKey("fallbacktolatestreplayid"));
        assertEquals("CUSTOM", parameters.get("replayPreset"));
        assertEquals("false", parameters.get("consumerWorkerPoolEnabled"));
        assertEquals("false", parameters.get("fallbackToLatestReplayId"));
    }

    private OffsetStorageReader reader(Map<String, String> expectedPartition, Map<String, Object> value) {
        return new OffsetStorageReader() {
            @Override
            public <T> Map<String, Object> offset(Map<String, T> partition) {
                assertEquals(expectedPartition, partition);
                return value;
            }

            @Override
            public <T> Map<Map<String, T>, Map<String, Object>> offsets(Collection<Map<String, T>> partitions) {
                throw new UnsupportedOperationException();
            }
        };
    }
}
