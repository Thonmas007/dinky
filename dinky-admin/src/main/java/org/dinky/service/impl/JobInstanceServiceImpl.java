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

package org.dinky.service.impl;

import org.dinky.api.FlinkAPI;
import org.dinky.assertion.Asserts;
import org.dinky.context.TenantContextHolder;
import org.dinky.daemon.pool.FlinkJobThreadPool;
import org.dinky.daemon.task.DaemonTask;
import org.dinky.daemon.task.DaemonTaskConfig;
import org.dinky.data.dto.ClusterConfigurationDTO;
import org.dinky.data.dto.JobDataDto;
import org.dinky.data.enums.GatewayType;
import org.dinky.data.enums.JobStatus;
import org.dinky.data.enums.Status;
import org.dinky.data.enums.TaskMonitorScanStatus;
import org.dinky.data.model.ClusterConfiguration;
import org.dinky.data.model.ClusterInstance;
import org.dinky.data.model.Task;
import org.dinky.data.model.ext.JobInfoDetail;
import org.dinky.data.model.home.JobInstanceCount;
import org.dinky.data.model.home.JobInstanceStatus;
import org.dinky.data.model.home.JobModelOverview;
import org.dinky.data.model.job.History;
import org.dinky.data.model.job.JobHistory;
import org.dinky.data.model.job.JobInstance;
import org.dinky.data.model.mapping.ClusterConfigurationMapping;
import org.dinky.data.model.mapping.ClusterInstanceMapping;
import org.dinky.data.result.ProTableResult;
import org.dinky.data.vo.task.JobInstanceVo;
import org.dinky.explainer.lineage.LineageBuilder;
import org.dinky.explainer.lineage.LineageResult;
import org.dinky.job.FlinkJobTask;
import org.dinky.mapper.JobInstanceMapper;
import org.dinky.mapper.TaskMapper;
import org.dinky.mybatis.service.impl.SuperServiceImpl;
import org.dinky.mybatis.util.ProTableUtil;
import org.dinky.service.ClusterConfigurationService;
import org.dinky.service.ClusterInstanceService;
import org.dinky.service.HistoryService;
import org.dinky.service.JobHistoryService;
import org.dinky.service.JobInstanceService;

import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import cn.hutool.core.util.StrUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * JobInstanceServiceImpl
 *
 * @since 2022/2/2 13:52
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class JobInstanceServiceImpl extends SuperServiceImpl<JobInstanceMapper, JobInstance>
        implements JobInstanceService {

    private static final Set<JobStatus> DISCOVERABLE_JOB_STATUSES = Collections.unmodifiableSet(EnumSet.of(
            JobStatus.INITIALIZING,
            JobStatus.CREATED,
            JobStatus.RUNNING,
            JobStatus.RESTARTING,
            JobStatus.RECONCILING));

    private final HistoryService historyService;
    private final ClusterInstanceService clusterInstanceService;
    private final ClusterConfigurationService clusterConfigurationService;
    private final JobHistoryService jobHistoryService;
    private final TaskMapper taskMapper;

    @Override
    public JobInstance getByIdWithoutTenant(Integer id) {
        return baseMapper.getByIdWithoutTenant(id);
    }

    @Override
    public JobInstanceStatus getStatusCount() {
        List<JobInstanceCount> jobInstanceCounts;
        jobInstanceCounts = baseMapper.countStatus();
        JobModelOverview modelOverview = baseMapper.getJobStreamingOrBatchModelOverview();
        JobInstanceStatus jobInstanceStatus = new JobInstanceStatus();
        jobInstanceStatus.setModelOverview(modelOverview);
        int total = 0;
        for (JobInstanceCount item : jobInstanceCounts) {
            Integer counts = Asserts.isNull(item.getCounts()) ? 0 : item.getCounts();
            total += counts;
            switch (JobStatus.get(item.getStatus())) {
                case INITIALIZING:
                    jobInstanceStatus.setInitializing(counts);
                    break;
                case RUNNING:
                    jobInstanceStatus.setRunning(counts);
                    break;
                case FINISHED:
                    jobInstanceStatus.setFinished(counts);
                    break;
                case FAILED:
                case FAILING:
                    jobInstanceStatus.setFailed(counts);
                    break;
                case CANCELED:
                    jobInstanceStatus.setCanceled(counts);
                    break;
                case RESTARTING:
                    jobInstanceStatus.setRestarting(counts);
                    break;
                case CREATED:
                    jobInstanceStatus.setCreated(counts);
                    break;
                case CANCELLING:
                    jobInstanceStatus.setCancelling(counts);
                    break;
                case SUSPENDED:
                    jobInstanceStatus.setSuspended(counts);
                    break;
                case RECONCILING:
                    jobInstanceStatus.setReconciling(counts);
                    break;
                case UNKNOWN:
                    jobInstanceStatus.setUnknown(counts);
                    break;
                default:
            }
        }
        jobInstanceStatus.setAll(total);
        return jobInstanceStatus;
    }

    @Override
    public List<JobInstance> listJobInstanceActive() {
        return baseMapper.listJobInstanceActive();
    }

    @Override
    public JobInfoDetail getJobInfoDetail(Integer id) {
        if (Asserts.isNull(TenantContextHolder.get())) {
            initTenantByJobInstanceId(id);
        }
        return getJobInfoDetailInfo(getById(id));
    }

    @Override
    public JobInfoDetail getJobInfoDetailInfo(JobInstance jobInstance) {
        Asserts.checkNull(jobInstance, Status.JOB_INSTANCE_NOT_EXIST.getMessage());

        JobInfoDetail jobInfoDetail = new JobInfoDetail(jobInstance.getId());

        jobInfoDetail.setInstance(jobInstance);

        ClusterInstance clusterInstance = clusterInstanceService.getById(jobInstance.getClusterId());
        jobInfoDetail.setClusterInstance(clusterInstance);

        History history = historyService.getById(jobInstance.getHistoryId());
        if (history != null) {
            history.setConfigJson(history.getConfigJson());
            jobInfoDetail.setHistory(history);
            if (Asserts.isNotNull(history.getClusterConfigurationId())) {
                ClusterConfiguration clusterConfig =
                        clusterConfigurationService.getClusterConfigById(history.getClusterConfigurationId());
                if (clusterConfig != null) {
                    jobInfoDetail.setClusterConfiguration(ClusterConfigurationDTO.fromBean(clusterConfig));
                }
            }
        }

        JobDataDto jobDataDto = jobHistoryService.getJobHistoryDto(jobInstance.getId());
        if (jobDataDto == null) {
            JobHistory jobHistory = JobHistory.builder()
                    .id(jobInstance.getId())
                    .clusterJson(ClusterInstanceMapping.getClusterInstanceMapping(clusterInstance))
                    .clusterConfigurationJson(
                            Asserts.isNotNull(jobInfoDetail.getClusterConfiguration())
                                    ? ClusterConfigurationMapping.getClusterConfigurationMapping(jobInfoDetail
                                            .getClusterConfiguration()
                                            .toBean())
                                    : null)
                    .build();
            jobHistoryService.save(jobHistory);
            jobDataDto = JobDataDto.fromJobHistory(jobHistory);
        }
        jobInfoDetail.setJobDataDto(jobDataDto);

        return jobInfoDetail;
    }

    @Override
    public JobInfoDetail refreshJobInfoDetail(Integer jobInstanceId, Integer taskId, boolean isForce) {
        DaemonTaskConfig daemonTaskConfig = DaemonTaskConfig.build(FlinkJobTask.TYPE, jobInstanceId, taskId);
        DaemonTask daemonTask = FlinkJobThreadPool.getInstance().getByTaskConfig(daemonTaskConfig);

        if (daemonTask != null && !isForce) {
            return ((FlinkJobTask) daemonTask).getJobInfoDetail();
        } else if (isForce) {
            FlinkJobThreadPool.getInstance().removeByTaskConfig(daemonTaskConfig);
            daemonTask = DaemonTask.build(daemonTaskConfig);
            daemonTask.dealTask();
            JobInfoDetail jobInfoDetail = ((FlinkJobTask) daemonTask).getJobInfoDetail();
            if (!JobStatus.isDone(jobInfoDetail.getInstance().getStatus())) {
                FlinkJobThreadPool.getInstance().execute(daemonTask);
            }
            return jobInfoDetail;
        } else {
            return getJobInfoDetail(jobInstanceId);
        }
    }

    /**
     * 节点驱逐可能让同一个 Kubernetes Application 以新 JID 恢复；这里从 overview 中选择最新活跃作业，
     * 并以条件更新保证后台扫描和人工点击并发执行时不会互相覆盖已经恢复的关联。
     */
    @Override
    @Transactional(rollbackFor = Exception.class)
    public JobInfoDetail discoverJobId(Integer jobInstanceId) {
        JobInfoDetail jobInfoDetail = getJobInfoDetail(jobInstanceId);
        JobInstance jobInstance = jobInfoDetail.getInstance();
        ClusterInstance clusterInstance = jobInfoDetail.getClusterInstance();
        if (clusterInstance == null
                || !GatewayType.get(clusterInstance.getType()).isKubernetesApplicationMode()) {
            log.warn("Job ID discovery only supports Kubernetes Application, job instance {}", jobInstanceId);
            return null;
        }

        String jobManagerHost = clusterInstance.getJobManagerHost();
        if (StrUtil.isBlank(jobManagerHost)) {
            log.warn("JobManager REST address is empty, cannot discover Job ID for instance {}", jobInstanceId);
            return null;
        }

        List<JsonNode> flinkJobs;
        try {
            flinkJobs = FlinkAPI.build(jobManagerHost).listJobs();
        } catch (Exception e) {
            // REST 暂不可达时保留旧关联，等待下一轮扫描，不能把网络故障误判成没有运行中的作业。
            log.warn(
                    "Query Flink job overview from {} failed for instance {}: {}",
                    jobManagerHost,
                    jobInstanceId,
                    e.toString());
            return null;
        }
        Optional<JsonNode> activeJob = findLatestActiveJob(flinkJobs, jobInstance.getName());
        if (!activeJob.isPresent()) {
            log.warn(
                    "No active Flink job named {} was found from {}, job instance {}",
                    jobInstance.getName(),
                    jobManagerHost,
                    jobInstanceId);
            return null;
        }

        JsonNode flinkJob = activeJob.get();
        String oldJobId = jobInstance.getJid();
        String newJobId = flinkJob.path("jid").asText();
        String newStatus = flinkJob.path("state").asText();

        LambdaUpdateWrapper<JobInstance> updateWrapper = new LambdaUpdateWrapper<JobInstance>()
                .eq(JobInstance::getId, jobInstanceId)
                .set(JobInstance::getJid, newJobId)
                .set(JobInstance::getStatus, newStatus)
                .set(JobInstance::getFinishTime, null)
                .set(JobInstance::getError, null);
        // 并发发现只允许旧 JID 或同一个新 JID 写入，避免迟到的扫描覆盖另一轮已经建立的新关联。
        updateWrapper.and(wrapper -> {
            if (StrUtil.isBlank(oldJobId)) {
                wrapper.isNull(JobInstance::getJid)
                        .or()
                        .eq(JobInstance::getJid, "")
                        .or()
                        .eq(JobInstance::getJid, newJobId);
            } else {
                wrapper.eq(JobInstance::getJid, oldJobId).or().eq(JobInstance::getJid, newJobId);
            }
        });
        int updatedRows = baseMapper.update(null, updateWrapper);
        if (updatedRows == 0) {
            JobInstance latestInstance = getById(jobInstanceId);
            if (latestInstance == null || !StrUtil.equals(newJobId, latestInstance.getJid())) {
                log.warn(
                        "Job ID discovery result {} was superseded for job instance {}",
                        newJobId,
                        jobInstanceId);
                return null;
            }
        }

        Task task = new Task();
        task.setId(jobInstance.getTaskId());
        task.setJobInstanceId(jobInstanceId);
        task.setMonitorScanStatus(TaskMonitorScanStatus.SUCCESS.getValue());
        taskMapper.updateById(task);
        log.info(
                "Discovered and relinked Flink Job ID for instance {}: {} -> {}, status {}",
                jobInstanceId,
                oldJobId,
                newJobId,
                newStatus);
        return getJobInfoDetail(jobInstanceId);
    }

    /** 同名作业可能残留多个历史记录，只允许选择最新的可恢复运行态，避免关联到已结束的旧作业。 */
    static Optional<JsonNode> findLatestActiveJob(List<JsonNode> jobs, String jobName) {
        if (jobs == null || StrUtil.isBlank(jobName)) {
            return Optional.empty();
        }
        return jobs.stream()
                .filter(Objects::nonNull)
                .filter(job -> StrUtil.equals(jobName, job.path("name").asText()))
                .filter(job -> DISCOVERABLE_JOB_STATUSES.contains(JobStatus.get(job.path("state").asText())))
                .filter(job -> StrUtil.isNotBlank(job.path("jid").asText()))
                .max(Comparator.comparingLong((JsonNode job) -> job.path("start-time").asLong(0L)));
    }

    @Override
    public boolean hookJobDone(String jobId, Integer taskId) {
        LambdaQueryWrapper<JobInstance> queryWrapper = new LambdaQueryWrapper<>();
        queryWrapper
                .eq(JobInstance::getJid, jobId)
                .eq(JobInstance::getTaskId, taskId)
                .orderByDesc(JobInstance::getCreateTime)
                .last("limit 1");
        JobInstance instance = baseMapper.selectOne(queryWrapper);
        if (instance == null) {
            // Not having a corresponding jobinstance means that this may not have succeeded in running,
            // returning true to prevent retry.
            return true;
        }

        DaemonTaskConfig config = DaemonTaskConfig.build(FlinkJobTask.TYPE, instance.getId(), instance.getTaskId());
        DaemonTask daemonTask = FlinkJobThreadPool.getInstance().removeByTaskConfig(config);
        daemonTask = Optional.ofNullable(daemonTask).orElse(DaemonTask.build(config));

        boolean isDone = daemonTask.dealTask();
        // If the task is not completed, it is re-queued
        if (!isDone) {
            daemonTask.dealTask();
            //            FlinkJobThreadPool.getInstance().execute(daemonTask);
        }
        return isDone;
    }

    @Override
    public boolean hookJobDoneByHistory(String jobId) {
        LambdaQueryWrapper<JobInstance> queryWrapper = new LambdaQueryWrapper<>();
        queryWrapper
                .eq(JobInstance::getJid, jobId)
                .orderByDesc(JobInstance::getCreateTime)
                .last("limit 1");
        JobInstance instance = baseMapper.selectOne(queryWrapper);

        if (instance == null
                || !StrUtil.equalsAny(
                        instance.getStatus(), JobStatus.RECONNECTING.getValue(), JobStatus.UNKNOWN.getValue())) {
            // Not having a corresponding jobinstance means that this may not have succeeded in running,
            // returning true to prevent retry.
            return true;
        }

        DaemonTaskConfig config = DaemonTaskConfig.build(FlinkJobTask.TYPE, instance.getId(), instance.getTaskId());
        DaemonTask daemonTask = FlinkJobThreadPool.getInstance().removeByTaskConfig(config);
        daemonTask = Optional.ofNullable(daemonTask).orElse(DaemonTask.build(config));

        boolean isDone = daemonTask.dealTask();
        // If the task is not completed, it is re-queued
        if (!isDone) {
            daemonTask.dealTask();
            //            FlinkJobThreadPool.getInstance().execute(daemonTask);
        }
        return isDone;
    }

    @Override
    public void refreshJobByTaskIds(Integer... taskIds) {
        for (Integer taskId : taskIds) {
            JobInstance instance = getJobInstanceByTaskId(taskId);
            DaemonTaskConfig daemonTaskConfig =
                    DaemonTaskConfig.build(FlinkJobTask.TYPE, instance.getId(), instance.getTaskId());
            FlinkJobThreadPool.getInstance().removeByTaskConfig(daemonTaskConfig);
            FlinkJobThreadPool.getInstance().execute(DaemonTask.build(daemonTaskConfig));
            refreshJobInfoDetail(instance.getId(), instance.getTaskId(), false);
        }
    }

    @Override
    public LineageResult getLineage(Integer id) {
        History history = getJobInfoDetail(id).getHistory();
        return LineageBuilder.getColumnLineageByLogicalPlan(history.getStatement(), history.getConfigJson());
    }

    @Override
    public JobInstance getJobInstanceByTaskId(Integer id) {
        return baseMapper.getJobInstanceByTaskId(id);
    }

    @Override
    public ProTableResult<JobInstanceVo> listJobInstances(JsonNode para) {
        int current = para.has("current") ? para.get("current").asInt() : 1;
        int pageSize = para.has("pageSize") ? para.get("pageSize").asInt() : 10;
        QueryWrapper<JobInstanceVo> queryWrapper = new QueryWrapper<>();
        ProTableUtil.autoQueryDefalut(para, queryWrapper);
        ObjectMapper mapper = new ObjectMapper();
        Map<String, Object> param = mapper.convertValue(para, Map.class);
        Page<JobInstanceVo> page = new Page<>(current, pageSize);
        List<JobInstanceVo> list = baseMapper.selectForProTable(page, queryWrapper, param);
        return ProTableResult.<JobInstanceVo>builder()
                .success(true)
                .data(list)
                .total(page.getTotal())
                .current(current)
                .pageSize(pageSize)
                .build();
    }

    @Override
    public void initTenantByJobInstanceId(Integer id) {
        Integer tenantId = baseMapper.getTenantByJobInstanceId(id);
        Asserts.checkNull(tenantId, Status.JOB_INSTANCE_NOT_EXIST.getMessage());
        TenantContextHolder.set(tenantId);
    }
}
