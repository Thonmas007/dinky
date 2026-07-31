# IDEA 导入 Dinky Flink 1.20 项目

当 IDEA 中出现 `org.apache.flink.configuration` 等包无法解析、代码大量报红，但命令行 Maven 可以正常编译时，通常是 IDEA 没有按构建脚本使用的 Maven Profile 导入项目。

## Maven 项目导入步骤

1. 在 IDEA 中打开项目根目录下的 `pom.xml`。

2. 右键单击根 `pom.xml`，选择 **Add as Maven Project**。

3. 打开 IDEA 右侧的 **Maven** 工具窗口。

4. 在 Maven 工具窗口的 **Profiles** 中勾选以下 Profile：

   - `aliyun`
   - `prod`
   - `web`
   - `flink-1.20`
   - `flink-single-version`

5. 确认没有勾选 `flink-1.16`，避免 IDEA 导入与实际构建版本不一致的 Flink 依赖。

6. 点击 Maven 工具窗口中的 **Reload All Maven Projects**，等待依赖索引和项目同步完成。

## JDK 11 配置

项目统一使用以下 JDK 11：

```text
/Users/liuningbo/applications/java/jdk11/Contents/Home
```

在 IDEA 中确认以下配置均使用 JDK 11：

- **Project Structure → Project SDK**
- **Settings → Build Tools → Maven → Importing → JDK for importer**
- **Settings → Build Tools → Maven → Runner → JRE**

## 仍然报红时

如果 Maven 重新加载完成后代码仍然报红，执行 **File → Invalidate Caches → Invalidate and Restart**，等待 IDEA 重启并重新建立索引。

> 清理缓存不能代替 Maven 项目导入。应先完成根 `pom.xml` 的导入和 Profile 配置，再清理缓存。
