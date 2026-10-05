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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.dinky.ws.GlobalWebSocketTopic;
import org.dinky.ws.WsSendEvent;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.test.util.ReflectionTestUtils;

class ScheduleMessageEventHandlerTest {
    // 一次状态采集异常后，下一周期必须继续发送，不能让小火苗永久停留在旧状态。
    @Test
    void shouldKeepPublishingAfterCollectionFailure() {
        ApplicationEventPublisher publisher = mock(ApplicationEventPublisher.class);
        ScheduleMessageEventHandler handler = new ScheduleMessageEventHandler() {
            private int attempts;

            @Override
            protected long scheduleDelay() {
                return 1000;
            }

            @Override
            public GlobalWebSocketTopic getTopic() {
                return GlobalWebSocketTopic.TASK_RUN_INSTANCE;
            }

            @Override
            public Map<String, Object> firstSubscribe(Set<String> params) {
                return Collections.emptyMap();
            }

            @Override
            public Map<String, Object> autoMessageSend() {
                if (attempts++ == 0) throw new IllegalStateException("temporary collection failure");
                return Collections.singletonMap("RunningTaskId", Collections.singleton(7));
            }
        };
        ReflectionTestUtils.setField(handler, "applicationEventPublisher", publisher);
        assertDoesNotThrow(handler::publishNextMessage);
        assertDoesNotThrow(handler::publishNextMessage);
        verify(publisher, times(1)).publishEvent(any(WsSendEvent.class));
    }
}
