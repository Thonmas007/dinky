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

package org.dinky.resource.impl;

import org.dinky.data.model.SystemConfiguration;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LocalResourceManagerTest {

    /**
     * 上传嵌套路径的资源时要自动补齐父目录，避免只建了数据库目录却无法真正落盘。
     */
    @Test
    void shouldCreateParentDirectoriesBeforeUploadingNestedFile(@TempDir Path tempDir) throws Exception {
        SystemConfiguration systemConfiguration = SystemConfiguration.getInstances();
        String originalUploadBasePath = systemConfiguration.getResourcesUploadBasePath().getValue();
        try {
            systemConfiguration.getResourcesUploadBasePath().setValue(tempDir.toString());

            LocalResourceManager resourceManager = new LocalResourceManager();
            resourceManager.putFile(
                    "nested/dir/ctl-flink-template.jar",
                    new ByteArrayInputStream("hello".getBytes(StandardCharsets.UTF_8)));

            Path storedFile = tempDir.resolve("nested/dir/ctl-flink-template.jar");
            assertTrue(Files.exists(storedFile));
            assertEquals("hello", Files.readString(storedFile, StandardCharsets.UTF_8));
        } finally {
            systemConfiguration.getResourcesUploadBasePath().setValue(originalUploadBasePath);
        }
    }
}
