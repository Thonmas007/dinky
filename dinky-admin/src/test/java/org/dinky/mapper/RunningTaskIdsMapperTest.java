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

package org.dinky.mapper;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.ibatis.builder.xml.XMLMapperBuilder;
import org.apache.ibatis.datasource.unpooled.UnpooledDataSource;
import org.apache.ibatis.mapping.Environment;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;
import org.apache.ibatis.transaction.jdbc.JdbcTransactionFactory;

import java.io.InputStream;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashSet;
import java.util.UUID;

import org.junit.jupiter.api.Test;

/** 实际 SQL 验证火苗不受历史运行记录、错绑和重扫队列影响。 */
class RunningTaskIdsMapperTest {
    @Test
    void shouldOnlyReturnLatestRunningTasks() throws Exception {
        Configuration configuration = new Configuration(new Environment(
                "test",
                new JdbcTransactionFactory(),
                new UnpooledDataSource("org.h2.Driver", "jdbc:h2:mem:" + UUID.randomUUID() + ";MODE=MySQL", "sa", "")));
        String resource = "mapper/JobInstanceMapper.xml";
        try (InputStream input = getClass().getClassLoader().getResourceAsStream(resource)) {
            new XMLMapperBuilder(input, configuration, resource, configuration.getSqlFragments()).parse();
        }
        try (SqlSession session =
                        new SqlSessionFactoryBuilder().build(configuration).openSession(true);
                Statement statement = session.getConnection().createStatement()) {
            statement.execute(
                    "create table dinky_task (id int primary key, tenant_id int, monitor_scan_status varchar(32))");
            statement.execute(
                    "create table dinky_job_instance (id int primary key, task_id int, tenant_id int, status varchar(32))");
            statement.execute(
                    "insert into dinky_task values (1,1,'SUCCESS'),(2,1,'SCANNING'),(3,1,'SUCCESS'),(4,1,'CANCELED'),(5,2,'SUCCESS')");
            statement.execute(
                    "insert into dinky_job_instance values (10,1,1,'RUNNING'),(11,1,1,'CANCELED'),(20,2,1,'UNKNOWN'),(30,3,1,'RUNNING'),(40,4,1,'RUNNING'),(50,5,2,'RUNNING')");
            assertEquals(
                    new HashSet<>(Arrays.asList(3, 5)),
                    new HashSet<>(session.getMapper(JobInstanceMapper.class).listRunningTaskIds()));
            // 最新实例转为重连或失败，即使旧实例 RUNNING 也必须立即消失。
            statement.execute("update dinky_job_instance set status='RECONNECTING' where id=30");
            statement.execute("update dinky_job_instance set status='FAILED' where id=50");
            session.clearCache();
            assertEquals(
                    0,
                    session.getMapper(JobInstanceMapper.class)
                            .listRunningTaskIds()
                            .size());
        }
    }
}
