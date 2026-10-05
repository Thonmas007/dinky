/*
 *
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.dinky.ws.handler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.dinky.controller.JobInstanceController;
import org.dinky.service.JobInstanceService;

import java.util.Collections;

import org.junit.jupiter.api.Test;

/** 运行状态变为空时必须主动推送，不能让客户端保留最后一批火苗。 */
class TaskRunInstanceTest {
    @Test
    void shouldBroadcastEmptyRunningTasksAfterStop() {
        JobInstanceService service = mock(JobInstanceService.class);
        TaskRunInstance handler = new TaskRunInstance(service);
        when(service.getRunningTaskIds()).thenReturn(Collections.singleton(11));
        assertEquals(
                Collections.singleton(11),
                handler.firstSubscribe(Collections.emptySet()).get("RunningTaskId"));
        assertEquals(Collections.singleton(11), handler.autoMessageSend().get("RunningTaskId"));
        when(service.getRunningTaskIds()).thenReturn(Collections.emptySet());
        assertEquals(Collections.emptySet(), handler.autoMessageSend().get("RunningTaskId"));
        assertEquals(
                Collections.emptySet(),
                handler.firstSubscribe(Collections.emptySet()).get("RunningTaskId"));
        assertEquals(
                Collections.emptySet(),
                new JobInstanceController(service).getRunningTaskIds().getData().get("RunningTaskId"));
        assertEquals(Collections.emptyMap(), handler.autoMessageSend());
    }
}
