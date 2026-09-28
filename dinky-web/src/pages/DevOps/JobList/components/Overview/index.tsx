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

import {
  AllJobIcons,
  BatchIcons,
  CancelIcons,
  ErrorIcons,
  FinishIcons,
  RestartIcons,
  RunningIcons,
  SteamIcons,
  UnknownIcons
} from '@/components/Icons/DevopsIcons';
import useHookRequest from '@/hooks/useHookRequest';
import { DevopsContext } from '@/pages/DevOps';
import { JOB_STATUS } from '@/pages/DevOps/constants';
import StatisticsCard from '@/pages/DevOps/JobList/components/Overview/StatisticsCard';
import { getData } from '@/services/api';
import { API_CONSTANTS } from '@/services/endpoints';
import { StatusCountOverView } from '@/types/Home/data';
import { l } from '@/utils/intl';
import { ProCard } from '@ant-design/pro-components';
import { Button, Col, Row, Space } from 'antd';
import { useContext } from 'react';

const JobOverview = (props: any) => {
  const { statusFilter, setStatusFilter } = useContext<any>(DevopsContext);
  const { data } = useHookRequest(getData, { defaultParams: [API_CONSTANTS.GET_STATUS_COUNT] });
  const statusCount = data as StatusCountOverView;

  return (
    <Row gutter={[8, 8]}>
      <Col span={24}>
        {/* 总计与各状态共用一行，压缩统计区高度，为任务列表保留更多空间。 */}
        <ProCard
          layout='center'
          boxShadow={true}
          style={{ width: '100%' }}
          bodyStyle={{ padding: 8 }}
        >
          <StatisticsCard
            compact
            title={l('devops.joblist.status.all')}
            value={statusCount?.all}
            icon={<AllJobIcons size={42} />}
            divider={true}
            atClick={() => {
              setStatusFilter(undefined);
            }}
            extra={
              <Space direction='vertical' size={0} align='start'>
                <Button size='small' type='text' icon={<BatchIcons size={16} />}>
                  {l('home.job.batch')}: {statusCount?.modelOverview?.batchJobCount}
                </Button>
                <Button size='small' type='text' icon={<SteamIcons size={16} />}>
                  {l('home.job.stream')}: {statusCount?.modelOverview?.streamingJobCount}
                </Button>
              </Space>
            }
          />
          <StatisticsCard
            compact
            title={l('devops.joblist.status.running')}
            value={statusCount?.running}
            icon={<RunningIcons size={42} />}
            atClick={() => {
              setStatusFilter(JOB_STATUS.RUNNING);
            }}
            isChecked={statusFilter === JOB_STATUS.RUNNING}
          />
          <StatisticsCard
            compact
            title={l('devops.joblist.status.cancelled')}
            value={statusCount?.canceled}
            icon={<CancelIcons size={42} />}
            atClick={() => {
              setStatusFilter(JOB_STATUS.CANCELED);
            }}
            isChecked={statusFilter === JOB_STATUS.CANCELED}
          />
          <StatisticsCard
            compact
            title={l('devops.joblist.status.failed')}
            value={statusCount?.failed}
            icon={<ErrorIcons size={42} />}
            atClick={() => {
              setStatusFilter(JOB_STATUS.FAILED);
            }}
            isChecked={statusFilter === JOB_STATUS.FAILED}
          />
          <StatisticsCard
            compact
            title={l('devops.joblist.status.restarting')}
            value={statusCount?.restarting}
            icon={<RestartIcons size={42} />}
            atClick={() => {
              setStatusFilter(JOB_STATUS.RESTARTING);
            }}
            isChecked={statusFilter === JOB_STATUS.RESTARTING}
          />
          <StatisticsCard
            compact
            title={l('devops.joblist.status.finished')}
            value={statusCount?.finished}
            icon={<FinishIcons size={42} />}
            atClick={() => {
              setStatusFilter(JOB_STATUS.FINISHED);
            }}
            isChecked={statusFilter === JOB_STATUS.FINISHED}
          />
          <StatisticsCard
            compact
            title={l('devops.joblist.status.unknown')}
            value={statusCount?.unknown}
            icon={<UnknownIcons size={42} />}
            divider={false}
            atClick={() => {
              setStatusFilter(JOB_STATUS.UNKNOWN);
            }}
            isChecked={statusFilter === JOB_STATUS.UNKNOWN}
          />
        </ProCard>
      </Col>
    </Row>
  );
};
export default JobOverview;
