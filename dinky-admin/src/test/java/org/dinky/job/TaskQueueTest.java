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

package org.dinky.job;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import org.dinky.daemon.entity.TaskQueue;
import org.dinky.daemon.task.DaemonTask;
import org.dinky.daemon.task.DaemonTaskConfig;

import org.junit.jupiter.api.Test;

class TaskQueueTest {

    // 运行状态采集持有快照时，新增、移除监控及外部修改快照均不能干扰队列。
    @Test
    void shouldReturnIndependentSnapshotForMonitoring() {
        TaskQueue<DaemonTask> queue = new TaskQueue<>();
        EqualTask task = new EqualTask(DaemonTaskConfig.build("test", 55, 12));
        queue.addTask(task);
        java.util.ArrayList<DaemonTask> snapshot = queue.getTasks();
        queue.removeByTask(task);
        assertEquals(1, snapshot.size());
        queue.addTask(task);
        snapshot.clear();
        assertEquals(1, queue.getTaskSize());
    }

    // 强制刷新替换监控后，旧工作线程的完成回调不能删除新对象。
    @Test
    void shouldKeepReplacementWhenOldEqualTaskCompletes() {
        TaskQueue<DaemonTask> queue = new TaskQueue<>();
        DaemonTaskConfig config = DaemonTaskConfig.build("test", 55, 12);
        EqualTask oldTask = new EqualTask(config);
        EqualTask newTask = new EqualTask(config);
        queue.addTask(oldTask);
        queue.removeByTaskConfig(config);
        queue.addTask(newTask);
        queue.removeByTask(oldTask);
        assertEquals(1, queue.getTaskSize());
        assertSame(newTask, queue.getByTaskConfig(config));
        queue.removeByTask(newTask);
        assertEquals(0, queue.getTaskSize());
    }

    // 模拟真实监控按配置判等，确保测试能捕获 equals 删除导致的竞争。
    private static class EqualTask implements DaemonTask {
        private DaemonTaskConfig config;

        EqualTask(DaemonTaskConfig config) {
            this.config = config;
        }

        @Override
        public DaemonTask setConfig(DaemonTaskConfig config) {
            this.config = config;
            return this;
        }

        @Override
        public DaemonTaskConfig getConfig() {
            return config;
        }

        @Override
        public String getType() {
            return "test";
        }

        @Override
        public boolean dealTask() {
            return true;
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof EqualTask && config.equals(((EqualTask) other).config);
        }

        @Override
        public int hashCode() {
            return config.hashCode();
        }
    }
}
