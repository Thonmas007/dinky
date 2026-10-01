/*
 *
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to you under the Apache License, Version 2.0
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

import CodeShow from '@/components/CustomEditor/CodeShow';
import { JobProps } from '@/pages/DevOps/JobDetail/data';
import { postAll } from '@/services/api';
import { API_CONSTANTS } from '@/services/endpoints';
import { ProCard } from '@ant-design/pro-components';
import {
  Alert,
  Button,
  Card,
  Checkbox,
  Input,
  List,
  Select,
  Space,
  Spin,
  Typography,
  message
} from 'antd';
import { ReloadOutlined } from '@ant-design/icons';
import { useCallback, useEffect, useMemo, useState } from 'react';

type CommandResult = {
  output: string;
  exitCode: number;
  timedOut: boolean;
};

const DEFAULT_NAMESPACE = 'flink-dev';

const isKubernetesName = (value: string) => /^[a-z0-9]([a-z0-9.-]*[a-z0-9])?$/.test(value);

const getJobClusterConfigurationId = (jobDetail: JobProps['jobDetail']) =>
  jobDetail?.history?.clusterConfigurationId ??
  jobDetail?.clusterInstance?.clusterConfigurationId ??
  jobDetail?.clusterConfiguration?.id;

const getJobNamespace = (jobDetail: JobProps['jobDetail']) => {
  const jobData = (jobDetail?.jobDataDto ?? {}) as any;
  const clusterConfiguration = (jobDetail?.clusterConfiguration ?? {}) as any;
  return (
    jobData?.clusterConfiguration?.configJson?.kubernetesConfig?.configuration?.[
      'kubernetes.namespace'
    ] ??
    clusterConfiguration?.config?.kubernetesConfig?.configuration?.['kubernetes.namespace'] ??
    DEFAULT_NAMESPACE
  );
};

/** 从作业详情提取 Kubernetes Application 可能使用的名称，兼容历史实例和 Flink REST 快照。 */
const getJobPodNameHints = (jobDetail: JobProps['jobDetail']) => {
  const jobData = (jobDetail?.jobDataDto ?? {}) as any;
  const config = (jobData?.config ?? {}) as any;
  return [
    jobDetail?.instance?.name,
    jobDetail?.history?.jobName,
    jobData?.job?.name,
    config?.name,
    config?.jobName
  ].filter((name): name is string => typeof name === 'string' && name.trim().length > 0);
};

/** Pod 名称通常以 Kubernetes Application 的作业名开头，避免误选同 namespace 的其他作业。 */
const findMatchingPod = (pods: string[], jobNameHints: string[]) => {
  const normalizedHints = jobNameHints.map((name) => name.trim()).filter(Boolean);
  return normalizedHints
    .map((hint) => pods.find((pod) => pod === hint || pod.startsWith(`${hint}-`)))
    .find(Boolean);
};

/** 通过现有只读 kubectl 接口列出作业 namespace 下的 Pod，避免用户离开作业详情页排查失败日志。 */
const KubernetesPodLogs = ({ jobDetail }: JobProps) => {
  const clusterConfigurationId = getJobClusterConfigurationId(jobDetail);
  const initialNamespace = useMemo(() => getJobNamespace(jobDetail), [jobDetail]);
  const jobNameHints = useMemo(() => getJobPodNameHints(jobDetail), [jobDetail]);
  const [namespace, setNamespace] = useState(initialNamespace);
  const [pods, setPods] = useState<string[]>([]);
  const [selectedPod, setSelectedPod] = useState<string>();
  const [listResult, setListResult] = useState<CommandResult>();
  const [logResult, setLogResult] = useState<CommandResult>();
  // 失败排查优先读取重启前实例；取消勾选后可查看当前容器输出。
  const [includePrevious, setIncludePrevious] = useState(true);
  const [loadingPods, setLoadingPods] = useState(false);
  const [loadingLogs, setLoadingLogs] = useState(false);

  const execute = useCallback(
    async (command: string) => {
      if (!clusterConfigurationId) {
        message.warning('当前作业没有关联 Kubernetes 集群配置');
        return undefined;
      }
      const response = await postAll(API_CONSTANTS.KUBERNETES_COMMAND_EXECUTE, {
        clusterConfigurationId,
        command
      });
      if (response?.code !== 0) {
        message.error(response?.msg || 'kubectl 命令执行失败');
        return undefined;
      }
      return response.data as CommandResult;
    },
    [clusterConfigurationId]
  );

  /** 刷新 Pod 列表并优先选中当前作业，避免同 namespace 多个作业时误看其他任务日志。 */
  const loadPods = useCallback(
    async (targetNamespace: string) => {
      const trimmedNamespace = targetNamespace.trim();
      if (!isKubernetesName(trimmedNamespace)) {
        message.warning('请输入合法的 Kubernetes namespace');
        return;
      }
      setLoadingPods(true);
      try {
        const result = await execute(`kubectl get pods -n ${trimmedNamespace} -o name`);
        if (!result) {
          return;
        }
        setListResult(result);
        const nextPods = (result.output || '')
          .split(/\r?\n/)
          .map((line) => line.trim())
          .filter((line) => line.startsWith('pod/'))
          .map((line) => line.slice('pod/'.length));
        setPods(nextPods);
        const matchedPod = findMatchingPod(nextPods, jobNameHints);
        setSelectedPod((current) => {
          if (matchedPod) {
            return matchedPod;
          }
          if (current && nextPods.includes(current) && findMatchingPod([current], jobNameHints)) {
            return current;
          }
          return nextPods.length === 1 ? nextPods[0] : undefined;
        });
      } finally {
        setLoadingPods(false);
      }
    },
    [execute, jobNameHints]
  );

  // 进入页签或切换作业后自动刷新，确保下拉框匹配当前作业的最新 Pod。
  useEffect(() => {
    setNamespace(initialNamespace);
    if (clusterConfigurationId && initialNamespace) {
      void loadPods(initialNamespace);
    }
  }, [clusterConfigurationId, initialNamespace, loadPods]);

  /** 读取选中 Pod 的最近日志；previous 用于容器重启后定位上一次失败原因。 */
  const loadLogs = async () => {
    const trimmedNamespace = namespace.trim();
    if (!selectedPod || !isKubernetesName(trimmedNamespace) || !isKubernetesName(selectedPod)) {
      message.warning('请选择 Pod 并确认 namespace 正确');
      return;
    }
    setLoadingLogs(true);
    try {
      const previousArg = includePrevious ? ' --previous' : '';
      const result = await execute(
        `kubectl logs -n ${trimmedNamespace} pod/${selectedPod} --all-containers=true --timestamps=true --tail=1000${previousArg}`
      );
      if (result) {
        setLogResult(result);
      }
    } finally {
      setLoadingLogs(false);
    }
  };

  return (
    <ProCard bodyStyle={{ height: parent.innerHeight - 200, overflow: 'auto' }}>
      <Space direction='vertical' size='middle' style={{ width: '100%' }}>
        <Alert
          type='info'
          showIcon
          message='Pod 日志'
          description='从当前作业关联的 Kubernetes 集群配置读取日志，不需要把 kubeconfig 下载到本地。'
        />
        <Space wrap>
          <Input
            value={namespace}
            onChange={(event) => setNamespace(event.target.value)}
            placeholder='namespace'
            addonBefore='Namespace'
            style={{ width: 260 }}
          />
          <Button
            type='primary'
            icon={<ReloadOutlined />}
            loading={loadingPods}
            onClick={() => void loadPods(namespace)}
          >
            刷新 Pod
          </Button>
          <Select
            value={selectedPod}
            onChange={setSelectedPod}
            options={pods.map((pod) => ({ value: pod, label: pod }))}
            placeholder='选择 Pod'
            showSearch
            style={{ minWidth: 360, maxWidth: '100%' }}
            notFoundContent='请先刷新 Pod'
          />
          <Checkbox
            checked={includePrevious}
            onChange={(event) => setIncludePrevious(event.target.checked)}
          >
            上一次容器日志
          </Checkbox>
          <Button
            icon={<ReloadOutlined />}
            loading={loadingLogs}
            disabled={!selectedPod}
            onClick={() => void loadLogs()}
          >
            查看日志
          </Button>
        </Space>
        {listResult && listResult.exitCode !== 0 && (
          <Alert
            type='warning'
            showIcon
            message='Pod 列表查询失败'
            description={listResult.output || '请确认集群配置和 namespace 是否仍然有效'}
          />
        )}
        {listResult?.exitCode === 0 && pods.length === 0 && (
          <Alert
            type='warning'
            showIcon
            message='当前 namespace 没有 Pod'
            description='如果失败任务的 Pod 已被 Kubernetes 清理，kubectl 无法恢复已删除 Pod 的日志，请改查集群日志平台或保留策略。'
          />
        )}
        {listResult?.exitCode === 0 && pods.length > 1 && !selectedPod && (
          <Alert
            type='info'
            showIcon
            message='未找到当前作业对应的 Pod'
            description='请手动选择 Pod；任务名称与 Pod 名称不一致时，系统不会默认选择其他作业。'
          />
        )}
        <Card title={selectedPod || '请选择 Pod'} bordered={false}>
          <Spin spinning={loadingLogs}>
            <CodeShow
              showFloatButton
              code={logResult?.output || '选择 Pod 后点击“查看日志”'}
              language='javalog'
              height='calc(100vh - 430px)'
            />
          </Spin>
          {logResult && logResult.exitCode !== 0 && (
            <Alert
              type='error'
              showIcon
              message={`Pod 日志读取失败（kubectl 退出码：${logResult.exitCode}）`}
              description={logResult.output || 'Pod 可能已被删除，或容器没有可读取的日志。'}
            />
          )}
        </Card>
      </Space>
    </ProCard>
  );
};

export default KubernetesPodLogs;
