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

import org.apache.iceberg.Table;
import org.junit.jupiter.api.Test;

/**
 * Control: identical run (same fault, same foreign commit, same restart and replay) but S1 is NOT
 * expired, so the floor is still reachable through the ancestry and the replay is filtered.
 */
class Mc3E2EControlNoExpiryTest extends Mc3E2EBase {

  @Override
  String variant() {
    return "control_foreign_append_no_expire";
  }

  @Override
  String maintenance(Table table) throws Exception {
    return foreignAppend(table) + "; (no expire_snapshots)";
  }

  @Test
  void floorReachableFiltersTheReplay() throws Exception {
    Result result = runScenario();
    report.println("   duplicate rows  = " + result.duplicateRows());
    report.println("   duplicate files = " + result.duplicateFiles());
    assertThat(result.replayPending).as("-coord checkpoint lags S1's DataWritten").isTrue();
    assertThat(result.duplicateRows()).as("no duplicates while the floor is reachable").isEmpty();
    for (int i = 0; i < BATCH; i += 1) {
      assertThat(result.rowsByKey.get("b1-" + i)).as("row b1-" + i).isEqualTo(1);
      assertThat(result.rowsByKey.get("b2-" + i)).as("row b2-" + i).isEqualTo(1);
    }
    report.println("RESULT control_foreign_append_no_expire: NO DUPLICATES (floor filtered replay)");
  }
}
