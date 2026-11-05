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

/** Bug variant A: another writer commits on top of S1, then expire_snapshots removes S1. */
class Mc3E2EForeignAppendExpiryTest extends Mc3E2EBase {

  @Override
  String variant() {
    return "foreign_append_then_expire";
  }

  @Override
  String maintenance(Table table) throws Exception {
    String a = foreignAppend(table);
    table.refresh();
    String b = expireAllButHead(table);
    return a + "; " + b;
  }

  @Test
  void replayedDataWrittenIsCommittedTwiceOnceFloorIsUnreachable() throws Exception {
    Result result = runScenario();
    report.println("   duplicate rows  = " + result.duplicateRows());
    report.println("   duplicate files = " + result.duplicateFiles());
    assertThat(result.replayPending).as("-coord checkpoint lags S1's DataWritten").isTrue();
    // bug triggered: every b1 row (committed source offsets) is now in the table twice
    for (int i = 0; i < BATCH; i += 1) {
      assertThat(result.rowsByKey.get("b1-" + i)).as("row b1-" + i).isEqualTo(2);
    }
    assertThat(result.sourceCommitted.get(0)).as("source offsets of b1+b2 committed").isEqualTo(10L);
    report.println("RESULT foreign_append_then_expire: DUPLICATES PRESENT -> MC-3 bug triggered");
  }
}
