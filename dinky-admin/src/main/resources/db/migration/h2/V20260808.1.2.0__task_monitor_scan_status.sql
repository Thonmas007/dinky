ALTER TABLE `dinky_task`
    ADD COLUMN IF NOT EXISTS `monitor_scan_status` varchar(32) DEFAULT 'NONE' COMMENT 'Kubernetes 监控重扫状态：NONE 未触发，SCANNING 扫描中，SUCCESS 已恢复，FAILED 扫描失败';
