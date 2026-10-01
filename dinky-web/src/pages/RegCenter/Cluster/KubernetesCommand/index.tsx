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
import SlowlyAppear from '@/components/Animation/SlowlyAppear';
import { queryDataByParams } from '@/services/BusinessCrud';
import { postAll } from '@/services/api';
import { API_CONSTANTS } from '@/services/endpoints';
import { PageContainer } from '@ant-design/pro-components';
import { Alert, Button, Card, Input, Select, Space, Tag, Typography, message } from 'antd';
import { useState } from 'react';
import { useAsyncEffect } from 'ahooks';

const { TextArea } = Input;

type CommandResult = {
  output: string;
  exitCode: number;
  timedOut: boolean;
};

type KubernetesClusterOption = {
  id: number;
  name: string;
  type: string;
  enabled: boolean;
  available: boolean;
};

/** 为运维人员提供受权限控制的 K8s 查询入口，并保留原始命令输出格式。 */
export default () => {
  const [command, setCommand] = useState('kubectl get pods -n flink-dev');
  const [configurations, setConfigurations] = useState<KubernetesClusterOption[]>([]);
  const [clusterConfigurationId, setClusterConfigurationId] = useState<number>();
  const [result, setResult] = useState<CommandResult>();
  const [loading, setLoading] = useState(false);

  useAsyncEffect(async () => {
    const options = await queryDataByParams<KubernetesClusterOption[]>(
      API_CONSTANTS.KUBERNETES_COMMAND_CONFIGURATIONS
    );
    const enabledOptions = options?.filter((option) => option.enabled) ?? [];
    setConfigurations(enabledOptions);
    if (enabledOptions.length > 0) {
      setClusterConfigurationId(enabledOptions[0].id);
    }
  }, []);

  /** 提交查询并保留后端返回的原始 kubectl 输出，便于直接定位集群状态。 */
  const execute = async () => {
    if (!command.trim()) {
      message.warning('请输入 kubectl 命令');
      return;
    }
    if (!clusterConfigurationId) {
      message.warning('请选择 Kubernetes 集群配置');
      return;
    }
    setLoading(true);
    try {
      const response = await postAll(API_CONSTANTS.KUBERNETES_COMMAND_EXECUTE, {
        clusterConfigurationId,
        command: command.trim()
      });
      if (response?.code === 0) {
        setResult(response.data);
      }
    } finally {
      setLoading(false);
    }
  };

  return (
    <SlowlyAppear>
      <PageContainer title={false}>
        <Card bordered={false}>
          <Space direction='vertical' size='middle' style={{ width: '100%' }}>
            <Space align='center'>
              <Typography.Title level={4} style={{ margin: 0 }}>
                K8s 查询
              </Typography.Title>
              <Tag color='blue'>只读</Tag>
            </Space>
            <Select
              value={clusterConfigurationId}
              onChange={setClusterConfigurationId}
              options={configurations.map((configuration) => ({
                value: configuration.id,
                label: `${configuration.name} (${configuration.type})`
              }))}
              placeholder='选择 Kubernetes 集群配置'
              style={{ width: 360, maxWidth: '100%' }}
              notFoundContent='暂无已启用的 Kubernetes 集群配置'
            />
            <TextArea
              value={command}
              onChange={(event) => setCommand(event.target.value)}
              onPressEnter={(event) => {
                if (!event.shiftKey) {
                  event.preventDefault();
                  void execute();
                }
              }}
              autoSize={{ minRows: 2, maxRows: 5 }}
              placeholder='kubectl get pods -n flink-dev'
              aria-label='kubectl 命令'
            />
            <Button type='primary' onClick={() => void execute()} loading={loading}>
              执行查询
            </Button>
            {result && (
              <>
                {result.timedOut && <Alert type='warning' showIcon message='命令执行超时' />}
                <Space>
                  <Typography.Text>退出码</Typography.Text>
                  <Tag color={result.exitCode === 0 ? 'success' : 'error'}>{result.exitCode}</Tag>
                </Space>
                <pre
                  style={{
                    margin: 0,
                    padding: 16,
                    minHeight: 280,
                    maxHeight: '60vh',
                    overflow: 'auto',
                    whiteSpace: 'pre-wrap',
                    wordBreak: 'break-word',
                    background: '#111827',
                    color: '#e5e7eb',
                    borderRadius: 4,
                    fontFamily: 'ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace'
                  }}
                >
                  {result.output || '命令没有返回内容'}
                </pre>
              </>
            )}
          </Space>
        </Card>
      </PageContainer>
    </SlowlyAppear>
  );
};
