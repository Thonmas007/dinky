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

import org.dinky.assertion.Asserts;
import org.dinky.context.SpringContextUtils;
import org.dinky.daemon.constant.FlinkTaskConstant;
import org.dinky.daemon.task.DaemonTask;
import org.dinky.daemon.task.DaemonTaskConfig;
import org.dinky.data.enums.GatewayType;
import org.dinky.data.enums.JobStatus;
import org.dinky.data.enums.TaskMonitorScanStatus;
import org.dinky.data.model.Task;
import org.dinky.data.model.SystemConfiguration;
import org.dinky.data.model.ext.JobInfoDetail;
import org.dinky.data.model.job.JobInstance;
import org.dinky.job.handler.JobAlertHandler;
import org.dinky.job.handler.JobMetricsHandler;
import org.dinky.job.handler.JobRefreshHandler;
import org.dinky.service.JobInstanceService;
import org.dinky.service.MonitorService;
import org.dinky.service.TaskService;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.context.annotation.DependsOn;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@DependsOn("springContextUtils")
@Slf4j
@Data
public class FlinkJobTask implements DaemonTask {

    // 节点拉起、镜像下载和 Flink 状态恢复可能持续数分钟，保留约 10 分钟自动发现窗口。
    private static final int MONITOR_SCAN_MAX_RETRY = 20;
    private static final long MONITOR_SCAN_RETRY_INTERVAL_MS = 30_000L;

    private DaemonTaskConfig config;

    public static final String TYPE = FlinkJobTask.class.toString();

    private static final JobInstanceService jobInstanceService;

    private static final MonitorService monitorService;

    private static final TaskService taskService;

    private long preDealTime;

    private long refreshCount = 0;

    private int monitorScanRetryCount = 0;

    private long lastMonitorScanTime = 0L;

    private Map<String, Map<String, String>> verticesAndMetricsMap = new ConcurrentHashMap<>();

    static {
        jobInstanceService = SpringContextUtils.getBean("jobInstanceServiceImpl", JobInstanceService.class);
        monitorService = SpringContextUtils.getBean("monitorServiceImpl", MonitorService.class);
        taskService = SpringContextUtils.getBean("taskServiceImpl", TaskService.class);
    }

    private JobInfoDetail jobInfoDetail;

    @Override
    public DaemonTask setConfig(DaemonTaskConfig config) {
        this.config = config;
        this.jobInfoDetail = jobInstanceService.getJobInfoDetail(config.getId());
        // Get a list of metrics and deduplicate them based on vertices and metrics
        monitorService
                .getMetricsLayoutByTaskId(jobInfoDetail.getInstance().getTaskId())
                .forEach(m -> {
                    verticesAndMetricsMap.putIfAbsent(m.getVertices(), new ConcurrentHashMap<>());
                    verticesAndMetricsMap.get(m.getVertices()).put(m.getMetrics(), "");
                });
        return this;
    }

    @Override
    public DaemonTaskConfig getConfig() {
        return config;
    }

    /**
     * Processing tasks.
     * <p>
     * Handle job refresh, alarm, monitoring and other actions
     * Returns true if the job has completed or exceeded the time to obtain data,
     * indicating that the processing is complete and moved out of the thread pool
     * Otherwise, false is returned, indicating that the processing is not completed and continues to remain in the thread pool
     * </p>
     *
     * @return Returns true if the job has completed, otherwise returns false
     */
    @Override
    public boolean dealTask() {
        volatilityBalance();
        if (isWaitingForMonitorScanRetry()) {
            return false;
        }

        boolean isDone = JobRefreshHandler.refreshJob(jobInfoDetail, isNeedSave());
        if (shouldRetryMonitorScan(isDone)) {
            if (isMonitorScanActive() && tryDiscoverJobId()) {
                return false;
            }
            isDone = handleMonitorScanRetry();
            if (!isDone) {
                return false;
            }
        } else {
            handleMonitorScanRecovered();
        }
        if (Asserts.isAllNotNull(jobInfoDetail.getClusterInstance())) {
            JobAlertHandler.getInstance().check(jobInfoDetail);
            if (SystemConfiguration.getInstances().getMetricsSysEnable().getValue()) {
                JobMetricsHandler.refreshAndWriteFlinkMetrics(jobInfoDetail, verticesAndMetricsMap);
            }
        }
        return isDone;
    }

    /**
     * Volatility balance.
     * <p>
     * This method is used to perform volatility equilibrium operations. Between each call,
     * by calculating the time interval between the current time and the last processing time,
     * If the interval is less than the set sleep time (TIME_SLEEP),
     * The thread sleeps for a period of time. Then update the last processing time to the current time.
     * </p>
     */
    public void volatilityBalance() {
        long gap = System.currentTimeMillis() - this.preDealTime;
        if (gap < FlinkTaskConstant.TIME_SLEEP) {
            try {
                Thread.sleep(FlinkTaskConstant.TIME_SLEEP);
            } catch (InterruptedException e) {
                log.error(e.getMessage(), e);
            }
        }
        preDealTime = System.currentTimeMillis();
    }

    /** Karpenter 驱逐后 Flink/TaskManager 可能短暂不可查，进入重扫窗口时先等待 30 秒再触发下一次扫描。 */
    private boolean isWaitingForMonitorScanRetry() {
        return isMonitorScanActive()
                && System.currentTimeMillis() - lastMonitorScanTime < MONITOR_SCAN_RETRY_INTERVAL_MS;
    }

    /** 仅对 Kubernetes Application 的监控不可达状态做重扫，避免影响 Flink 已明确返回的真实终态。 */
    private boolean shouldRetryMonitorScan(boolean isDone) {
        if (!isKubernetesApplicationJob()) {
            return false;
        }
        String status = jobInfoDetail.getInstance().getStatus();
        return JobStatus.RECONNECTING.getValue().equals(status)
                || (isDone && JobStatus.UNKNOWN.getValue().equals(status));
    }

    /** 监控失败先保留实例关联，按 30 秒间隔持续重扫约 10 分钟，全部失败后才停止监控并标记任务扫描失败。 */
    private boolean handleMonitorScanRetry() {
        if (!isMonitorScanActive()) {
            markTaskMonitorScanStatus(TaskMonitorScanStatus.SCANNING);
            keepJobReconnecting();
            lastMonitorScanTime = System.currentTimeMillis();
            log.warn(
                    "Kubernetes application job monitor failed, start rescan task {} job instance {}",
                    jobInfoDetail.getInstance().getTaskId(),
                    jobInfoDetail.getInstance().getId());
            return false;
        }

        monitorScanRetryCount++;
        if (monitorScanRetryCount >= MONITOR_SCAN_MAX_RETRY) {
            markTaskMonitorScanStatus(TaskMonitorScanStatus.FAILED);
            log.warn(
                    "Kubernetes application job monitor rescan failed after {} retries, task {} job instance {}",
                    MONITOR_SCAN_MAX_RETRY,
                    jobInfoDetail.getInstance().getTaskId(),
                    jobInfoDetail.getInstance().getId());
            resetMonitorScanRetry();
            return true;
        }

        keepJobReconnecting();
        lastMonitorScanTime = System.currentTimeMillis();
        log.warn(
                "Kubernetes application job monitor rescan failed, will retry {}/{} later, task {} job instance {}",
                monitorScanRetryCount,
                MONITOR_SCAN_MAX_RETRY,
                jobInfoDetail.getInstance().getTaskId(),
                jobInfoDetail.getInstance().getId());
        return false;
    }

    /** 每轮延迟重扫额外查询 overview；JobManager 重建产生新 JID 时立即替换内存详情并继续监控。 */
    private boolean tryDiscoverJobId() {
        String oldJobId = jobInfoDetail.getInstance().getJid();
        try {
            JobInfoDetail discoveredJob =
                    jobInstanceService.discoverJobId(jobInfoDetail.getInstance().getId());
            if (discoveredJob == null) {
                return false;
            }
            jobInfoDetail = discoveredJob;
            log.info(
                    "Kubernetes application job monitor discovered Job ID, task {} instance {}: {} -> {}",
                    jobInfoDetail.getInstance().getTaskId(),
                    jobInfoDetail.getInstance().getId(),
                    oldJobId,
                    jobInfoDetail.getInstance().getJid());
            resetMonitorScanRetry();
            return true;
        } catch (Exception e) {
            log.warn(
                    "Discover Kubernetes application Job ID failed for instance {}: {}",
                    jobInfoDetail.getInstance().getId(),
                    e.toString());
            return false;
        }
    }

    /** 重扫期间一旦重新查到 Flink 作业，就恢复任务关联状态并继续原有监控流程。 */
    private void handleMonitorScanRecovered() {
        if (!isMonitorScanActive()) {
            return;
        }
        markTaskMonitorScanStatus(TaskMonitorScanStatus.SUCCESS);
        log.info(
                "Kubernetes application job monitor rescan recovered, task {} job instance {}",
                jobInfoDetail.getInstance().getTaskId(),
                jobInfoDetail.getInstance().getId());
        resetMonitorScanRetry();
    }

    private boolean isKubernetesApplicationJob() {
        return Asserts.isNotNull(jobInfoDetail.getClusterInstance())
                && GatewayType.get(jobInfoDetail.getClusterInstance().getType()).isKubernetesApplicationMode();
    }

    private boolean isMonitorScanActive() {
        return lastMonitorScanTime > 0;
    }

    private void resetMonitorScanRetry() {
        monitorScanRetryCount = 0;
        lastMonitorScanTime = 0L;
    }

    private void keepJobReconnecting() {
        jobInfoDetail.getInstance().setStatus(JobStatus.RECONNECTING.getValue());
        jobInfoDetail.getInstance().setFinishTime(LocalDateTime.now());
        // 只持久化重连字段，避免后台持有的旧详情把人工发现后更新的新 JID 覆盖回去。
        JobInstance reconnectingInstance = new JobInstance();
        reconnectingInstance.setId(jobInfoDetail.getInstance().getId());
        reconnectingInstance.setStatus(JobStatus.RECONNECTING.getValue());
        reconnectingInstance.setFinishTime(jobInfoDetail.getInstance().getFinishTime());
        jobInstanceService.updateById(reconnectingInstance);
    }

    private void markTaskMonitorScanStatus(TaskMonitorScanStatus status) {
        Task task = new Task();
        task.setId(jobInfoDetail.getInstance().getTaskId());
        task.setMonitorScanStatus(status.getValue());
        if (TaskMonitorScanStatus.SCANNING == status || TaskMonitorScanStatus.SUCCESS == status) {
            task.setJobInstanceId(jobInfoDetail.getInstance().getId());
        }
        taskService.updateById(task);
    }

    /**
     * Determine if you need to save.
     * <p>
     * This method is used to determine whether saving is required.
     * According to the value of the refresh count,
     * if the refreshCount is divisible by 60, it returns true, indicating that it needs to be saved.
     * At the same time, if you need to save, reset refreshCount to 0 to restart the count.
     * Finally, increment refreshCount by 1.
     * </p>
     *
     * @return Returns true if you need to save, otherwise returns false
     */
    public boolean isNeedSave() {
        boolean isNeed = refreshCount % 60 == 0;
        if (isNeed) {
            refreshCount = 0;
        }
        refreshCount++;
        return isNeed;
    }

    /**
     * Determine whether objects are equal.
     * <p>
     * This method is used to determine whether the current object is equal to the given object.
     * If two object references are the same, return true directly.
     * Returns false if the given object is null or the class of the given object is not the same as the class of the current object.
     * Otherwise, convert the given object to the FlinkJobTask type and compare the config IDs of the two objects equal.
     * In FlinkJobTask, the ID of the config is unique, so only need to compare whether the ID of the config is equal or not.
     * If the objects are judged to be equal, the same task will join the thread pool multiple times.
     * </p>
     *
     * @param obj object to compare
     * @return Returns true if two objects are equal, otherwise returns false
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        FlinkJobTask other = (FlinkJobTask) obj;
        return Objects.equals(config.getId(), other.config.getId());
    }

    @Override
    public String getType() {
        return TYPE;
    }
}
