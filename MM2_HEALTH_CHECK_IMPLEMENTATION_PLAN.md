# MM2 Deterministic Health Check - Implementation Plan

## Overview

| Item | Value |
|------|-------|
| **Endpoint** | `GET /health` |
| **Response** | `{ "healthy": boolean, "details": {...} }` |
| **Scope** | All flows in the MM2 instance |
| **Checks** | 8 deterministic health checks |

---

## Problem Statement

MirrorMaker 2 instances lack an easy way to determine if an instance is healthy. This implementation adds a `GET /health` endpoint that provides:

1. A simple boolean `healthy` field for monitoring/alerting
2. Fine-grained diagnostics for troubleshooting

---

## Files to Create

| File | Purpose |
|------|---------|
| `connect/mirror/src/main/java/org/apache/kafka/connect/mirror/rest/resources/MirrorHealthResource.java` | REST endpoint handler |
| `connect/mirror/src/main/java/org/apache/kafka/connect/mirror/rest/entities/MirrorHealthResponse.java` | Response DTO |
| `connect/mirror/src/main/java/org/apache/kafka/connect/mirror/rest/entities/FlowHealthStatus.java` | Per-flow health details |
| `connect/mirror/src/main/java/org/apache/kafka/connect/mirror/rest/entities/CheckResult.java` | Individual check result |
| `connect/mirror/src/main/java/org/apache/kafka/connect/mirror/health/MirrorHealthChecker.java` | Core health check logic |
| `connect/mirror/src/main/java/org/apache/kafka/connect/mirror/health/HeartbeatFreshnessChecker.java` | Heartbeat topic consumer |
| `connect/mirror/src/main/java/org/apache/kafka/connect/mirror/health/MetricsAccessor.java` | Access to MM2 metrics |

## Files to Modify

| File | Change |
|------|--------|
| `connect/mirror/src/main/java/org/apache/kafka/connect/mirror/rest/MirrorRestServer.java` | Register `MirrorHealthResource` |
| `connect/mirror/src/main/java/org/apache/kafka/connect/mirror/MirrorMaker.java` | Pass additional dependencies to REST server |

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    GET /health                               │
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│                  MirrorHealthResource                        │
│  - Receives HTTP request                                     │
│  - Returns MirrorHealthResponse                              │
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│                  MirrorHealthChecker                         │
│  - Orchestrates all health checks                            │
│  - Aggregates results per flow                               │
└─────────────────────────────────────────────────────────────┘
                            │
        ┌───────────────────┼───────────────────┐
        ▼                   ▼                   ▼
┌──────────────┐   ┌──────────────┐   ┌──────────────┐
│   Herder     │   │  Heartbeat   │   │   Metrics    │
│   Status     │   │  Checker     │   │   Accessor   │
└──────────────┘   └──────────────┘   └──────────────┘
```

---

## Health Checks Specification

### Check 1: Connectors Running

```java
/**
 * Check: All 3 connectors (Source, Heartbeat, Checkpoint) are in RUNNING state
 * 
 * Data Source: Herder.connectorStatus(connectorName).connector().state()
 * Deterministic: YES
 * 
 * Healthy when: state == "RUNNING" for all connectors
 * Unhealthy when: state == "FAILED" | "PAUSED" | "UNASSIGNED"
 */
CheckResult checkConnectorsRunning(Herder herder) {
    for (String connector : CONNECTOR_NAMES) {
        ConnectorStateInfo status = herder.connectorStatus(connector);
        if (!"RUNNING".equals(status.connector().state())) {
            return unhealthy(connector + " is " + status.connector().state());
        }
    }
    return healthy();
}
```

---

### Check 2: Tasks Healthy

```java
/**
 * Check: No tasks in FAILED state
 * 
 * Data Source: Herder.connectorStatus(connectorName).tasks()
 * Deterministic: YES
 * 
 * Healthy when: No task has state == "FAILED"
 * Unhealthy when: Any task state == "FAILED"
 */
CheckResult checkTasksHealthy(Herder herder) {
    for (String connector : CONNECTOR_NAMES) {
        ConnectorStateInfo status = herder.connectorStatus(connector);
        for (TaskState task : status.tasks()) {
            if ("FAILED".equals(task.state())) {
                return unhealthy(connector + " task " + task.id() + " FAILED: " + task.trace());
            }
        }
    }
    return healthy();
}
```

---

### Check 3: Source Tasks Exist (Conditional)

```java
/**
 * Check: MirrorSourceConnector has ≥1 running task (if topics should exist)
 * 
 * Data Source: Herder.connectorStatus("MirrorSourceConnector").tasks()
 *              + AdminClient.listTopics() on source
 * Deterministic: YES
 * 
 * Healthy when: 
 *   - taskCount > 0, OR
 *   - taskCount == 0 AND no topics match filter (expected)
 * Unhealthy when:
 *   - taskCount == 0 AND topics exist that match filter
 */
CheckResult checkSourceTasksExist(Herder herder, Admin sourceAdmin, TopicFilter filter) {
    ConnectorStateInfo status = herder.connectorStatus("MirrorSourceConnector");
    int runningTasks = (int) status.tasks().stream()
        .filter(t -> "RUNNING".equals(t.state()))
        .count();
    
    if (runningTasks > 0) {
        return healthy();
    }
    
    // Check if topics exist that should be replicated
    Set<String> sourceTopics = sourceAdmin.listTopics().names().get();
    boolean hasMatchingTopics = sourceTopics.stream()
        .anyMatch(filter::shouldReplicateTopic);
    
    if (hasMatchingTopics) {
        return unhealthy("No source tasks running but " + 
            "topics exist that match filter");
    }
    return healthy(); // No matching topics, 0 tasks is correct
}
```

---

### Check 4: Internal Topics Exist

```java
/**
 * Check: Required internal topics exist on target cluster
 * 
 * Data Source: AdminClient.listTopics() on target
 * Deterministic: YES
 * 
 * Topics to check:
 *   - mm2-offsets.{source}.internal
 *   - mm2-status.{source}.internal
 *   - mm2-configs.{source}.internal
 *   - {source}.heartbeats (if heartbeats enabled)
 *   - {source}.checkpoints (if checkpoints enabled)
 */
CheckResult checkInternalTopicsExist(Admin targetAdmin, SourceAndTarget flow, Config config) {
    Set<String> existingTopics = targetAdmin.listTopics().names().get();
    List<String> missing = new ArrayList<>();
    
    String source = flow.source();
    
    // Always required
    checkTopic(existingTopics, "mm2-offsets." + source + ".internal", missing);
    checkTopic(existingTopics, "mm2-status." + source + ".internal", missing);
    checkTopic(existingTopics, "mm2-configs." + source + ".internal", missing);
    
    // Conditional
    if (config.emitHeartbeatsEnabled()) {
        checkTopic(existingTopics, source + ".heartbeats", missing);
    }
    if (config.emitCheckpointsEnabled()) {
        checkTopic(existingTopics, source + ".checkpoints", missing);
    }
    
    if (!missing.isEmpty()) {
        return unhealthy("Missing internal topics: " + missing);
    }
    return healthy();
}
```

---

### Check 5: Heartbeat Freshness (Conditional)

```java
/**
 * Check: Latest heartbeat is recent
 * 
 * Data Source: Consumer reading from {source}.heartbeats topic
 * Deterministic: YES
 * 
 * Healthy when: latestHeartbeatTimestamp > (now - 2 * heartbeatInterval)
 * Unhealthy when: No recent heartbeat
 * Skip when: emit.heartbeats.enabled = false
 */
CheckResult checkHeartbeatFresh(HeartbeatFreshnessChecker checker, 
                                 SourceAndTarget flow, Config config) {
    if (!config.emitHeartbeatsEnabled()) {
        return skipped("Heartbeats disabled");
    }
    
    String heartbeatTopic = flow.source() + ".heartbeats";
    long heartbeatInterval = config.emitHeartbeatsIntervalMs();
    long threshold = heartbeatInterval * 2;
    
    OptionalLong latestTimestamp = checker.getLatestHeartbeatTimestamp(heartbeatTopic);
    
    if (latestTimestamp.isEmpty()) {
        return unhealthy("No heartbeats found in " + heartbeatTopic);
    }
    
    long age = System.currentTimeMillis() - latestTimestamp.getAsLong();
    if (age > threshold) {
        return unhealthy("Heartbeat is " + age + "ms old (threshold: " + threshold + "ms)");
    }
    return healthy();
}
```

---

### Check 6: Records Flowing (Conditional)

```java
/**
 * Check: Data is being replicated (or source is caught up)
 * 
 * Data Source: MirrorSourceMetrics.record-rate + offset comparison
 * Deterministic: YES
 * 
 * Healthy when:
 *   - record-rate > 0, OR
 *   - record-rate == 0 AND source offsets == consumed offsets (caught up)
 * Unhealthy when:
 *   - record-rate == 0 AND behind on offsets
 */
CheckResult checkRecordsFlowing(MetricsAccessor metrics, Herder herder, 
                                 Admin sourceAdmin, SourceAndTarget flow) {
    double recordRate = metrics.getRecordRate(flow);
    
    if (recordRate > 0) {
        return healthy();
    }
    
    // Rate is 0 - check if we're caught up or stuck
    ConnectorStateInfo status = herder.connectorStatus("MirrorSourceConnector");
    if (status.tasks().isEmpty()) {
        return skipped("No source tasks"); // Covered by check 3
    }
    
    // Compare source end offsets vs consumed offsets
    // This requires reading from offset storage
    Map<TopicPartition, Long> sourceEndOffsets = getSourceEndOffsets(sourceAdmin);
    Map<TopicPartition, Long> consumedOffsets = getConsumedOffsets(herder);
    
    for (TopicPartition tp : sourceEndOffsets.keySet()) {
        long endOffset = sourceEndOffsets.get(tp);
        long consumed = consumedOffsets.getOrDefault(tp, 0L);
        if (consumed < endOffset - LAG_TOLERANCE) {
            return unhealthy("Partition " + tp + " is behind: " + 
                consumed + " < " + endOffset);
        }
    }
    return healthy(); // Caught up, rate 0 is fine
}
```

---

### Check 7: Replication Latency Acceptable

```java
/**
 * Check: Replication latency is within acceptable threshold
 * 
 * Data Source: MirrorSourceMetrics.replication-latency-ms-avg
 * Deterministic: YES
 * 
 * Healthy when: avgLatency < configuredThreshold
 * Unhealthy when: avgLatency >= configuredThreshold
 * Skip when: No data flowing (covered by check 6)
 */
CheckResult checkLatencyAcceptable(MetricsAccessor metrics, 
                                    SourceAndTarget flow, Config config) {
    double recordRate = metrics.getRecordRate(flow);
    if (recordRate == 0) {
        return skipped("No records flowing");
    }
    
    double avgLatency = metrics.getReplicationLatencyAvg(flow);
    long threshold = config.healthLatencyThresholdMs(); // New config
    
    if (avgLatency > threshold) {
        return unhealthy("Replication latency " + avgLatency + 
            "ms exceeds threshold " + threshold + "ms");
    }
    return healthy();
}
```

---

### Check 8: Error Metrics Not Elevated

```java
/**
 * Check: Task error metrics are not elevated
 * 
 * Data Source: ErrorHandlingMetrics per task
 * Deterministic: YES
 * 
 * Healthy when:
 *   - lastErrorTimestamp is not recent (> 5 min ago), AND
 *   - retry rate is not elevated
 * Unhealthy when: Recent errors or elevated retries
 */
CheckResult checkErrorMetrics(MetricsAccessor metrics, SourceAndTarget flow, Config config) {
    long lastErrorTime = metrics.getLastErrorTimestamp(flow);
    long errorRecencyThreshold = config.healthErrorRecencyMs(); // e.g., 5 minutes
    
    if (lastErrorTime > 0) {
        long timeSinceError = System.currentTimeMillis() - lastErrorTime;
        if (timeSinceError < errorRecencyThreshold) {
            return unhealthy("Recent error " + timeSinceError + "ms ago");
        }
    }
    
    long totalRetries = metrics.getTotalRetries(flow);
    long totalFailures = metrics.getTotalRecordFailures(flow);
    
    // These are cumulative - would need to track deltas for rate
    // For simplicity, just check if failures exist
    if (totalFailures > 0) {
        return warning("Total record failures: " + totalFailures);
    }
    
    return healthy();
}
```

---

## Response Structure

### MirrorHealthResponse.java

```java
public class MirrorHealthResponse {
    private final boolean healthy;
    private final Map<String, FlowHealthStatus> flows;
    private final long timestamp;
}
```

### FlowHealthStatus.java

```java
public class FlowHealthStatus {
    private final boolean healthy;
    private final Map<String, CheckResult> checks;
}
```

### CheckResult.java

```java
public class CheckResult {
    private final Status status;  // HEALTHY, UNHEALTHY, WARNING, SKIPPED
    private final String message; // null if healthy
}

public enum Status {
    HEALTHY,
    UNHEALTHY,
    WARNING,
    SKIPPED
}
```

### Example JSON Response

```json
{
  "healthy": false,
  "timestamp": 1703789012345,
  "flows": {
    "A->B": {
      "healthy": false,
      "checks": {
        "connectorsRunning": { "status": "HEALTHY" },
        "tasksHealthy": { "status": "HEALTHY" },
        "sourceTasksExist": { "status": "HEALTHY" },
        "internalTopicsExist": { "status": "HEALTHY" },
        "heartbeatFresh": { "status": "UNHEALTHY", "message": "Heartbeat is 65000ms old (threshold: 60000ms)" },
        "recordsFlowing": { "status": "HEALTHY" },
        "latencyAcceptable": { "status": "HEALTHY" },
        "errorMetrics": { "status": "HEALTHY" }
      }
    },
    "B->A": {
      "healthy": true,
      "checks": {
        "connectorsRunning": { "status": "HEALTHY" },
        "tasksHealthy": { "status": "HEALTHY" },
        "sourceTasksExist": { "status": "HEALTHY" },
        "internalTopicsExist": { "status": "HEALTHY" },
        "heartbeatFresh": { "status": "HEALTHY" },
        "recordsFlowing": { "status": "HEALTHY" },
        "latencyAcceptable": { "status": "HEALTHY" },
        "errorMetrics": { "status": "HEALTHY" }
      }
    }
  }
}
```

---

## New Configuration Options

| Config | Default | Description |
|--------|---------|-------------|
| `health.latency.threshold.ms` | `60000` | Max acceptable replication latency |
| `health.error.recency.ms` | `300000` | Time window for "recent" errors (5 min) |
| `health.lag.tolerance` | `100` | Offset lag tolerance for "caught up" |
| `health.require.matching.topics` | `true` | Fail if no topics match filter |

---

## Class Implementations

### MirrorHealthChecker.java

```java
package org.apache.kafka.connect.mirror.health;

import org.apache.kafka.connect.mirror.MirrorMakerConfig;
import org.apache.kafka.connect.mirror.SourceAndTarget;
import org.apache.kafka.connect.mirror.rest.entities.CheckResult;
import org.apache.kafka.connect.mirror.rest.entities.FlowHealthStatus;
import org.apache.kafka.connect.mirror.rest.entities.MirrorHealthResponse;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class MirrorHealthChecker implements AutoCloseable {
    
    private static final String[] CONNECTOR_NAMES = {
        "MirrorSourceConnector",
        "MirrorHeartbeatConnector", 
        "MirrorCheckpointConnector"
    };
    
    private final Map<SourceAndTarget, Herder> herders;
    private final MirrorMakerConfig config;
    private final HeartbeatFreshnessChecker heartbeatChecker;
    private final MetricsAccessor metricsAccessor;
    
    public MirrorHealthChecker(Map<SourceAndTarget, Herder> herders,
                                MirrorMakerConfig config) {
        this.herders = herders;
        this.config = config;
        this.heartbeatChecker = new HeartbeatFreshnessChecker(config);
        this.metricsAccessor = new MetricsAccessor(herders);
    }
    
    public MirrorHealthResponse checkHealth() {
        Map<String, FlowHealthStatus> flowStatuses = new HashMap<>();
        boolean allHealthy = true;
        
        for (Map.Entry<SourceAndTarget, Herder> entry : herders.entrySet()) {
            SourceAndTarget flow = entry.getKey();
            Herder herder = entry.getValue();
            
            FlowHealthStatus status = checkFlowHealth(flow, herder);
            flowStatuses.put(flow.toString(), status);
            
            if (!status.isHealthy()) {
                allHealthy = false;
            }
        }
        
        return new MirrorHealthResponse(allHealthy, flowStatuses, System.currentTimeMillis());
    }
    
    private FlowHealthStatus checkFlowHealth(SourceAndTarget flow, Herder herder) {
        Map<String, CheckResult> checks = new LinkedHashMap<>();
        
        // Run all checks
        checks.put("connectorsRunning", checkConnectorsRunning(herder));
        checks.put("tasksHealthy", checkTasksHealthy(herder));
        checks.put("sourceTasksExist", checkSourceTasksExist(flow, herder));
        checks.put("internalTopicsExist", checkInternalTopicsExist(flow));
        checks.put("heartbeatFresh", checkHeartbeatFresh(flow));
        checks.put("recordsFlowing", checkRecordsFlowing(flow, herder));
        checks.put("latencyAcceptable", checkLatencyAcceptable(flow));
        checks.put("errorMetrics", checkErrorMetrics(flow));
        
        boolean healthy = checks.values().stream()
            .allMatch(r -> r.getStatus() != CheckResult.Status.UNHEALTHY);
        
        return new FlowHealthStatus(healthy, checks);
    }
    
    private CheckResult checkConnectorsRunning(Herder herder) {
        for (String connector : CONNECTOR_NAMES) {
            try {
                ConnectorStateInfo status = herder.connectorStatus(connector);
                String state = status.connector().state();
                if (!"RUNNING".equals(state)) {
                    return CheckResult.unhealthy(connector + " is " + state);
                }
            } catch (Exception e) {
                return CheckResult.unhealthy(connector + " status unavailable: " + e.getMessage());
            }
        }
        return CheckResult.healthy();
    }
    
    private CheckResult checkTasksHealthy(Herder herder) {
        for (String connector : CONNECTOR_NAMES) {
            try {
                ConnectorStateInfo status = herder.connectorStatus(connector);
                for (ConnectorStateInfo.TaskState task : status.tasks()) {
                    if ("FAILED".equals(task.state())) {
                        return CheckResult.unhealthy(connector + " task " + task.id() + 
                            " FAILED: " + task.trace());
                    }
                }
            } catch (Exception e) {
                return CheckResult.unhealthy(connector + " task status unavailable: " + e.getMessage());
            }
        }
        return CheckResult.healthy();
    }
    
    private CheckResult checkSourceTasksExist(SourceAndTarget flow, Herder herder) {
        // Implementation as specified above
        return CheckResult.healthy(); // Placeholder
    }
    
    private CheckResult checkInternalTopicsExist(SourceAndTarget flow) {
        // Implementation as specified above
        return CheckResult.healthy(); // Placeholder
    }
    
    private CheckResult checkHeartbeatFresh(SourceAndTarget flow) {
        // Implementation as specified above
        return CheckResult.healthy(); // Placeholder
    }
    
    private CheckResult checkRecordsFlowing(SourceAndTarget flow, Herder herder) {
        // Implementation as specified above
        return CheckResult.healthy(); // Placeholder
    }
    
    private CheckResult checkLatencyAcceptable(SourceAndTarget flow) {
        // Implementation as specified above  
        return CheckResult.healthy(); // Placeholder
    }
    
    private CheckResult checkErrorMetrics(SourceAndTarget flow) {
        // Implementation as specified above
        return CheckResult.healthy(); // Placeholder
    }
    
    @Override
    public void close() {
        heartbeatChecker.close();
    }
}
```

---

### HeartbeatFreshnessChecker.java

```java
package org.apache.kafka.connect.mirror.health;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.connect.mirror.MirrorMakerConfig;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;

public class HeartbeatFreshnessChecker implements AutoCloseable {
    
    private final Map<String, KafkaConsumer<byte[], byte[]>> consumers;
    private final MirrorMakerConfig config;
    
    public HeartbeatFreshnessChecker(MirrorMakerConfig config) {
        this.config = config;
        this.consumers = new ConcurrentHashMap<>();
    }
    
    public OptionalLong getLatestHeartbeatTimestamp(String heartbeatTopic, 
                                                     String targetClusterAlias) {
        KafkaConsumer<byte[], byte[]> consumer = getOrCreateConsumer(targetClusterAlias);
        
        try {
            // Get end offset
            TopicPartition tp = new TopicPartition(heartbeatTopic, 0);
            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(List.of(tp));
            long endOffset = endOffsets.getOrDefault(tp, 0L);
            
            if (endOffset == 0) {
                return OptionalLong.empty(); // No records
            }
            
            // Seek to last record
            consumer.assign(List.of(tp));
            consumer.seek(tp, endOffset - 1);
            
            ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofSeconds(5));
            if (records.isEmpty()) {
                return OptionalLong.empty();
            }
            
            ConsumerRecord<byte[], byte[]> lastRecord = records.iterator().next();
            return OptionalLong.of(lastRecord.timestamp());
        } catch (Exception e) {
            return OptionalLong.empty();
        }
    }
    
    private KafkaConsumer<byte[], byte[]> getOrCreateConsumer(String clusterAlias) {
        return consumers.computeIfAbsent(clusterAlias, alias -> {
            Map<String, Object> consumerConfig = new java.util.HashMap<>(
                config.clusterProps(alias));
            consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "mm2-health-checker-" + 
                System.currentTimeMillis());
            consumerConfig.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
            consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
            return new KafkaConsumer<>(consumerConfig, 
                new ByteArrayDeserializer(), new ByteArrayDeserializer());
        });
    }
    
    @Override
    public void close() {
        consumers.values().forEach(consumer -> {
            try {
                consumer.close(Duration.ofSeconds(5));
            } catch (Exception e) {
                // Ignore
            }
        });
        consumers.clear();
    }
}
```

---

### MetricsAccessor.java

```java
package org.apache.kafka.connect.mirror.health;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.connect.mirror.SourceAndTarget;
import org.apache.kafka.connect.runtime.ConnectMetrics;
import org.apache.kafka.connect.runtime.Herder;

import java.util.Map;

public class MetricsAccessor {
    
    private static final String SOURCE_CONNECTOR_GROUP = "MirrorSourceConnector";
    
    private final Map<SourceAndTarget, Herder> herders;
    
    public MetricsAccessor(Map<SourceAndTarget, Herder> herders) {
        this.herders = herders;
    }
    
    public double getRecordRate(SourceAndTarget flow) {
        Herder herder = herders.get(flow);
        if (herder == null) {
            return 0.0;
        }
        
        ConnectMetrics connectMetrics = herder.connectMetrics();
        Metrics metrics = connectMetrics.metrics();
        
        // Aggregate record-rate across all partitions for this flow
        double totalRate = 0.0;
        for (Map.Entry<MetricName, KafkaMetric> entry : metrics.metrics().entrySet()) {
            MetricName name = entry.getKey();
            if (SOURCE_CONNECTOR_GROUP.equals(name.group()) && 
                "record-rate".equals(name.name()) &&
                flow.source().equals(name.tags().get("source")) &&
                flow.target().equals(name.tags().get("target"))) {
                
                Object value = entry.getValue().metricValue();
                if (value instanceof Number) {
                    totalRate += ((Number) value).doubleValue();
                }
            }
        }
        return totalRate;
    }
    
    public double getReplicationLatencyAvg(SourceAndTarget flow) {
        Herder herder = herders.get(flow);
        if (herder == null) {
            return 0.0;
        }
        
        ConnectMetrics connectMetrics = herder.connectMetrics();
        Metrics metrics = connectMetrics.metrics();
        
        // Find max latency across partitions (worst case)
        double maxLatency = 0.0;
        for (Map.Entry<MetricName, KafkaMetric> entry : metrics.metrics().entrySet()) {
            MetricName name = entry.getKey();
            if (SOURCE_CONNECTOR_GROUP.equals(name.group()) && 
                "replication-latency-ms-avg".equals(name.name()) &&
                flow.source().equals(name.tags().get("source")) &&
                flow.target().equals(name.tags().get("target"))) {
                
                Object value = entry.getValue().metricValue();
                if (value instanceof Number) {
                    maxLatency = Math.max(maxLatency, ((Number) value).doubleValue());
                }
            }
        }
        return maxLatency;
    }
    
    public long getLastErrorTimestamp(SourceAndTarget flow) {
        // Access ErrorHandlingMetrics - implementation depends on 
        // how error metrics are organized per task
        return 0L; // Placeholder
    }
    
    public long getTotalRetries(SourceAndTarget flow) {
        // Sum total-retries across all tasks in this flow
        return 0L; // Placeholder
    }
    
    public long getTotalRecordFailures(SourceAndTarget flow) {
        // Sum total-record-failures across all tasks in this flow
        return 0L; // Placeholder
    }
}
```

---

### MirrorHealthResource.java

```java
package org.apache.kafka.connect.mirror.rest.resources;

import org.apache.kafka.connect.mirror.health.MirrorHealthChecker;
import org.apache.kafka.connect.mirror.rest.entities.MirrorHealthResponse;

import io.swagger.v3.oas.annotations.Operation;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

@Path("/health")
@Produces(MediaType.APPLICATION_JSON)
public class MirrorHealthResource {

    private final MirrorHealthChecker healthChecker;

    @Inject
    public MirrorHealthResource(MirrorHealthChecker healthChecker) {
        this.healthChecker = healthChecker;
    }

    @GET
    @Operation(summary = "Check health of all MirrorMaker 2 replication flows")
    public Response healthCheck() {
        MirrorHealthResponse response = healthChecker.checkHealth();
        
        int statusCode = response.isHealthy() 
            ? Response.Status.OK.getStatusCode()
            : Response.Status.SERVICE_UNAVAILABLE.getStatusCode();
        
        return Response.status(statusCode).entity(response).build();
    }
}
```

---

### Modified: MirrorRestServer.java

```java
package org.apache.kafka.connect.mirror.rest;

import org.apache.kafka.connect.mirror.MirrorMakerConfig;
import org.apache.kafka.connect.mirror.SourceAndTarget;
import org.apache.kafka.connect.mirror.health.MirrorHealthChecker;
import org.apache.kafka.connect.mirror.rest.resources.InternalMirrorResource;
import org.apache.kafka.connect.mirror.rest.resources.MirrorHealthResource;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.rest.RestClient;
import org.apache.kafka.connect.runtime.rest.RestServer;
import org.apache.kafka.connect.runtime.rest.RestServerConfig;

import org.glassfish.hk2.api.TypeLiteral;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public class MirrorRestServer extends RestServer {

    private final RestClient restClient;
    private Map<SourceAndTarget, Herder> herders;
    private MirrorHealthChecker healthChecker;

    public MirrorRestServer(Map<?, ?> props, RestClient restClient) {
        super(RestServerConfig.forInternal(props));
        this.restClient = restClient;
    }

    // MODIFIED: Added config parameter
    public void initializeInternalResources(Map<SourceAndTarget, Herder> herders,
                                            MirrorMakerConfig config) {
        this.herders = herders;
        this.healthChecker = new MirrorHealthChecker(herders, config);
        super.initializeResources();
    }

    @Override
    protected Collection<Class<?>> regularResources() {
        return List.of(
            InternalMirrorResource.class,
            MirrorHealthResource.class  // NEW
        );
    }

    @Override
    protected Collection<Class<?>> adminResources() {
        return List.of();
    }

    @Override
    protected void configureRegularResources(ResourceConfig resourceConfig) {
        resourceConfig.register(new Binder());
    }

    private class Binder extends AbstractBinder {
        @Override
        protected void configure() {
            bind(herders).to(new TypeLiteral<Map<SourceAndTarget, Herder>>() { });
            bind(restClient).to(RestClient.class);
            bind(healthChecker).to(MirrorHealthChecker.class);  // NEW
        }
    }
    
    // NEW: Cleanup on shutdown
    @Override
    public void stop() {
        super.stop();
        if (healthChecker != null) {
            healthChecker.close();
        }
    }
}
```

---

## Testing Strategy

### Unit Tests

| Test Class | Coverage |
|------------|----------|
| `MirrorHealthCheckerTest` | Each check method with mocked Herder/Admin |
| `HeartbeatFreshnessCheckerTest` | Consumer logic with embedded Kafka |
| `MetricsAccessorTest` | Metric aggregation logic |
| `MirrorHealthResourceTest` | HTTP response codes and JSON structure |

### Integration Tests

| Test Scenario | Expected Result |
|---------------|-----------------|
| All healthy | `200 OK`, `healthy: true` |
| Connector FAILED | `503`, identifies which connector |
| Task FAILED | `503`, identifies which task with trace |
| Heartbeat stale | `503`, shows age vs threshold |
| No matching topics (expected) | `200 OK`, `healthy: true` |
| No matching topics (unexpected) | `503`, explains filter issue |
| High latency | `503`, shows latency vs threshold |
| Recent errors | `503`, shows error timestamp |

### Failure Injection Tests

| Injection | Validation |
|-----------|------------|
| Stop source cluster | Errors detected within threshold |
| Kill connector | Connector state changes detected |
| Pause connector | State change detected |
| Network partition | Heartbeat staleness detected |
| Slow source cluster | Latency threshold exceeded |

---

## Implementation Order

| Phase | Tasks | Estimated Effort |
|-------|-------|------------------|
| **Phase 1** | Core structure: Resource, Response DTOs, basic connector/task checks | 2-3 days |
| **Phase 2** | Internal topics check, topic filter validation | 1 day |
| **Phase 3** | Heartbeat freshness checker (requires consumer) | 2 days |
| **Phase 4** | Metrics accessor (record-rate, latency, errors) | 2-3 days |
| **Phase 5** | Offset comparison for "caught up" detection | 1-2 days |
| **Phase 6** | Configuration options, documentation | 1 day |
| **Phase 7** | Testing (unit + integration) | 3-4 days |

**Total Estimated Effort: 12-16 days**

---

## Health Check Summary

| Check | Deterministic | Catches |
|-------|---------------|---------|
| Connectors Running | ✅ Yes | Connector failures, pauses |
| Tasks Healthy | ✅ Yes | Task crashes, failures |
| Source Tasks Exist | ✅ Yes | Config errors, no matching topics |
| Internal Topics Exist | ✅ Yes | Startup failures |
| Heartbeat Fresh | ✅ Yes | Connectivity issues, stuck replication |
| Records Flowing | ✅ Yes | Stuck replication with backlog |
| Latency Acceptable | ✅ Yes | Slow replication, SLA violations |
| Error Metrics | ✅ Yes | Transient errors, retries |

---

## Future Enhancements

1. **Checkpoint freshness** - Similar to heartbeat check
2. **Offset sync verification** - Ensure offset mappings are current
3. **Per-topic health** - Break down health by replicated topic
4. **Historical trending** - Track health over time
5. **Alerting integration** - Webhook callbacks on state changes

