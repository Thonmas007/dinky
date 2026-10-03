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
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.UUID;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.baomidou.mybatisplus.extension.plugins.MybatisPlusInterceptor;
import com.baomidou.mybatisplus.extension.plugins.handler.TenantLineHandler;
import com.baomidou.mybatisplus.extension.plugins.inner.TenantLineInnerInterceptor;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;

/** 使用实际 Mapper SQL 验证错绑修复的并发约束，不把 Mockito 返回成功当作数据库校验。 */
class TaskMonitorRecoveryMapperTest {
    private SqlSession session;
    private TaskMapper mapper;

    @BeforeEach
    void initializeDatabase() throws Exception {
        UnpooledDataSource dataSource =
                new UnpooledDataSource("org.h2.Driver", "jdbc:h2:mem:" + UUID.randomUUID() + ";MODE=MySQL", "sa", "");
        Configuration configuration =
                new Configuration(new Environment("test", new JdbcTransactionFactory(), dataSource));
        MybatisPlusInterceptor interceptor = new MybatisPlusInterceptor();
        interceptor.addInnerInterceptor(new TenantLineInnerInterceptor(new TenantLineHandler() {
            @Override
            public Expression getTenantId() {
                return new LongValue(1);
            }
        }));
        configuration.addInterceptor(interceptor);
        String resource = "mapper/TaskMapper.xml";
        try (InputStream input = getClass().getClassLoader().getResourceAsStream(resource)) {
            new XMLMapperBuilder(input, configuration, resource, configuration.getSqlFragments()).parse();
        }
        session = new SqlSessionFactoryBuilder().build(configuration).openSession(true);
        mapper = session.getMapper(TaskMapper.class);
        execute(
                "create table dinky_task (id int primary key, job_instance_id int, monitor_scan_status varchar(32), tenant_id int)");
        execute("create table dinky_job_instance (id int primary key, task_id int, tenant_id int)");
        execute("insert into dinky_task values (11, 41, 'SCANNING', 1), (99, 90, 'SCANNING', 2)");
        execute("insert into dinky_job_instance values (41, 11, 1), (51, 11, 1), (80, 12, 1), (90, 99, 2)");
    }

    @AfterEach
    void closeDatabase() {
        if (session != null) {
            session.close();
        }
    }

    @Test
    void shouldRecoverLatestSubmissionFromHistoricalBinding() throws Exception {
        assertEquals(1, recover());
        assertEquals(51, binding());
    }

    @Test
    void shouldRejectHistoricalInstanceEvenWhenItOwnsBinding() throws Exception {
        assertEquals(0, mapper.recoverLatestJobInstance(11, 41, 41, "SCANNING", "SUCCESS"));
        assertEquals(41, binding());
    }

    @Test
    void shouldRejectSubmissionCreatedDuringDiscoveryBeforeItsBindingIsWritten() throws Exception {
        execute("insert into dinky_job_instance values (52, 11, 1)");
        assertEquals(0, recover());
        assertEquals(41, binding());
    }

    @Test
    void shouldPreserveConcurrentRebinding() throws Exception {
        execute("update dinky_task set job_instance_id = 52 where id = 11");
        assertEquals(0, recover());
        assertEquals(52, binding());
    }

    @Test
    void shouldPreserveManualStopDuringDiscovery() throws Exception {
        execute("update dinky_task set monitor_scan_status = 'CANCELED' where id = 11");
        assertEquals(0, recover());
        assertEquals(41, binding());
    }

    @Test
    void shouldRejectInstanceBelongingToAnotherTask() {
        assertEquals(0, mapper.recoverLatestJobInstance(11, 80, 41, "SCANNING", "SUCCESS"));
    }

    @Test
    void shouldPreserveTenantIsolationInConditionalRecovery() {
        assertEquals(0, mapper.recoverLatestJobInstance(99, 90, 90, "SCANNING", "SUCCESS"));
    }

    @Test
    void shouldRecoverNullLegacyBinding() throws Exception {
        execute("update dinky_task set job_instance_id = null, monitor_scan_status = null where id = 11");
        assertEquals(1, mapper.recoverLatestJobInstance(11, 51, null, null, "SUCCESS"));
        assertEquals(51, binding());
    }

    private int recover() {
        return mapper.recoverLatestJobInstance(11, 51, 41, "SCANNING", "SUCCESS");
    }

    private int binding() throws Exception {
        try (Statement statement = session.getConnection().createStatement();
                ResultSet result = statement.executeQuery("select job_instance_id from dinky_task where id = 11")) {
            result.next();
            return result.getInt(1);
        }
    }

    private void execute(String sql) throws Exception {
        try (Statement statement = session.getConnection().createStatement()) {
            statement.execute(sql);
        }
    }
}
