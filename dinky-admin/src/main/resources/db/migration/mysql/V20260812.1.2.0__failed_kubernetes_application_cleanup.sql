SET NAMES utf8mb4;

ALTER TABLE `dinky_job_instance`
    ADD COLUMN `failed_cleanup_status` varchar(32) NULL DEFAULT NULL COMMENT 'FAILED Kubernetes Application 清理状态：PENDING 待清理，RUNNING 清理中，SUCCEEDED 已清理，SKIPPED 已重提跳过，EXHAUSTED 重试耗尽' AFTER `failed_restart_count`,
    ADD COLUMN `failed_cleanup_after` datetime NULL DEFAULT NULL COMMENT 'FAILED Kubernetes Application 日志保留结束后的清理时间' AFTER `failed_cleanup_status`,
    ADD COLUMN `failed_cleanup_attempts` int NOT NULL DEFAULT 0 COMMENT 'FAILED Kubernetes Application 清理尝试次数，最多 3 次' AFTER `failed_cleanup_after`,
    ADD INDEX `job_instance_failed_cleanup_idx` (`status`, `failed_cleanup_status`, `failed_cleanup_after`);
