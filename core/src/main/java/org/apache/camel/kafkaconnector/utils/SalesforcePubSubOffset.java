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
import java.net.URISyntaxException;
import java.util.Base64;
import java.util.Map;

import org.apache.camel.Exchange;
import org.apache.camel.util.URISupport;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.storage.OffsetStorageReader;

public final class SalesforcePubSubOffset {
    private static final String PREFIX = "salesforce:pubSubSubscribe:";
    private static final String REPLAY_ID_HEADER = "CamelSalesforcePubSubReplayId";
    private static final String REPLAY_ID = "replayId";

    private final String url;
    private final Map<String, String> partition;

    public SalesforcePubSubOffset(String url, String org) {
        if (org == null || org.isBlank()) {
            throw new ConnectException("Salesforce Pub/Sub offset partition must be a non-empty org identifier");
        }
        if (url == null || !url.startsWith(PREFIX)) {
            throw new ConnectException("Salesforce Pub/Sub restart checkpoints require a salesforce:pubSubSubscribe: source URL");
        }
        String channel = URISupport.stripQuery(url).substring(PREFIX.length());
        if (!channel.startsWith("/") || channel.contains("{{") || channel.contains("#") || channel.isBlank()) {
            throw new ConnectException("Salesforce Pub/Sub restart checkpoints require a literal Salesforce channel");
        }
        this.url = url;
        this.partition = Map.of("type", "salesforce-pubsub", "org", org, "channel", channel);
    }

    public Map<String, String> partition() {
        return partition;
    }

    public Map<String, String> offset(Exchange exchange) {
        Object value = exchange.getMessage().getHeader(REPLAY_ID_HEADER);
        if (!(value instanceof String) || ((String) value).isBlank()) {
            throw new ConnectException("Salesforce Pub/Sub event has no valid " + REPLAY_ID_HEADER + " header");
        }
        validateReplayId((String) value);
        return Map.of(REPLAY_ID, (String) value);
    }

    public String resumeUrl(OffsetStorageReader reader) {
        Map<String, Object> offset = reader.offset(partition);
        try {
            URI uri = new URI(URISupport.normalizeUri(url));
            Map<String, Object> parameters = URISupport.parseParameters(uri);
            if (parameters.keySet().stream()
                    .anyMatch(key -> "consumerWorkerPoolExecutorService".equalsIgnoreCase(key))) {
                throw new ConnectException("Salesforce Pub/Sub restart checkpoints cannot use a custom consumer executor");
            }
            parameters.keySet().removeIf(key -> "consumerWorkerPoolEnabled".equalsIgnoreCase(key)
                    || "fallbackToLatestReplayId".equalsIgnoreCase(key));
            parameters.put("consumerWorkerPoolEnabled", "false");
            parameters.put("fallbackToLatestReplayId", "false");
            if (offset != null) {
                Object replayId = offset.get(REPLAY_ID);
                if (!(replayId instanceof String) || ((String) replayId).isBlank()) {
                    throw new ConnectException("Stored Salesforce Pub/Sub offset has no valid replayId");
                }
                validateReplayId((String) replayId);
                parameters.keySet().removeIf(key -> "replayPreset".equalsIgnoreCase(key) || "pubSubReplayId".equalsIgnoreCase(key));
                parameters.put("replayPreset", "CUSTOM");
                parameters.put("pubSubReplayId", "RAW(" + replayId + ")");
            }
            return URISupport.createRemainingURI(uri, parameters).toString();
        } catch (URISyntaxException e) {
            throw new ConnectException("Unable to apply Salesforce Pub/Sub replay position to source URL", e);
        }
    }

    private static void validateReplayId(String replayId) {
        try {
            Base64.getDecoder().decode(replayId);
        } catch (IllegalArgumentException e) {
            throw new ConnectException("Salesforce Pub/Sub replayId is not valid Base64", e);
        }
    }
}
