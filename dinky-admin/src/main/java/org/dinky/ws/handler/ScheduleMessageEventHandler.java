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

import org.dinky.assertion.Asserts;
import org.dinky.ws.GlobalWebSocketTopic;
import org.dinky.ws.WsSendEvent;

import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import javax.annotation.Resource;

import org.springframework.context.ApplicationEventPublisher;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class ScheduleMessageEventHandler implements WsMessageEventHandler {
    @Resource
    private ApplicationEventPublisher applicationEventPublisher;

    /**
     *
     * @return timed scheduling intervals; Unit: milliseconds
     */
    protected abstract long scheduleDelay();

    @Override
    public void run() {
        Timer timer = new Timer();
        long delay = scheduleDelay();
        timer.schedule(
                new TimerTask() {
                    @Override
                    public void run() {
                        publishNextMessage();
                    }
                },
                0,
                delay);
    }

    // Timer 遇到未捕获异常会永久退出，单次采集或发布失败不能停止后续运行状态推送。
    protected void publishNextMessage() {
        GlobalWebSocketTopic topic = getTopic();
        try {
            Map<String, ?> data = autoMessageSend();
            if (Asserts.isNotNullMap(data)) {
                WsSendEvent event =
                        WsSendEvent.builder().topic(topic).paramsAndData(data).build();
                applicationEventPublisher.publishEvent(event);
            }
        } catch (Exception e) {
            log.warn("WebSocket topic {} collection failed; retry next cycle", topic, e);
        }
    }
}
