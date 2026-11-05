/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.connect.mc3;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.junit.jupiter.api.Test;

/**
 * Escalation Level 0 (pure black box, no fault injected anywhere): healthy commit S1, table
 * maintenance (other writer + expire_snapshots), graceful task restart through REST. Expected: the
 * floor becomes unreachable, but no committed DataWritten is replayed (the -coord checkpoint was
 * advanced after S1), so no duplicate appears -- the defect stays latent without a replay trigger.
 */
class Mc3E2ELevel0NoFaultTest extends Mc3E2EBase {

  @Override
  String variant() {
    return "level0_no_fault_graceful_restart";
  }

  @Override
  String maintenance(Table table) throws Exception {
    String a = foreignAppend(table);
    table.refresh();
    return a + "; " + expireAllButHead(table);
  }

  @Test
  void noFaultNoReplayNoDuplicate() throws Exception {
    step("0. start connector");
    connect.configureConnector(CONNECTOR, connectorConfig());
    connect
        .assertions()
        .assertConnectorAndExactlyNumTasksAreRunning(CONNECTOR, 1, "Connector did not start");
    produceBatch("b1");
    await(
        "healthy connector snapshot S1",
        Duration.ofSeconds(120),
        () -> !connectorSnapshots(table()).isEmpty());
    Snapshot s1 = connectorSnapshots(table()).get(0);
    report.println("   S1 = " + describe(s1));
    List<String> ctl = new ArrayList<>();
    long dw = dumpControlTopic(s1.summary().get(COMMIT_ID_PROP), ctl);
    await(
        "-coord checkpoint past S1's DataWritten",
        Duration.ofSeconds(60),
        () -> {
          try {
            return groupOffsets(COORD_GROUP).getOrDefault(0, -1L) > dw;
          } catch (Exception e) {
            return false;
          }
        });
    report.println(
        "   S1 DataWritten @" + dw + ", -coord checkpoint = " + groupOffsets(COORD_GROUP));

    step("1. maintenance: " + variant());
    report.println("   " + maintenance(table()));
    report.println("   floor now = " + floor(table()));

    step("2. graceful restart of the healthy task (REST), then batch b2");
    connect.restartTask(CONNECTOR, 0);
    awaitTaskState("RUNNING", Duration.ofSeconds(90));
    produceBatch("b2");
    await(
        "all b2 rows committed",
        Duration.ofSeconds(120),
        () -> {
          Map<String, Integer> rows = rows(table());
          for (int i = 0; i < BATCH; i += 1) {
            if (!rows.containsKey("b2-" + i)) {
              return false;
            }
          }
          return true;
        });
    Thread.sleep(8000L);

    step("3. result");
    Map<String, Integer> rows = rows(table());
    report.println("   rows by data key = " + rows);
    report.println("   live data-file multiplicity = " + liveFiles(table()));
    for (int i = 0; i < BATCH; i += 1) {
      assertThat(rows.get("b1-" + i)).as("row b1-" + i).isEqualTo(1);
    }
    report.println("RESULT level0: floor unreachable but NO replay -> no duplicate (not triggered)");
  }
}
