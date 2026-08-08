SET NAMES utf8mb4;

DELIMITER $$
DROP PROCEDURE IF EXISTS add_column_if_not_exists$$
CREATE PROCEDURE add_column_if_not_exists(
    IN tableName VARCHAR(64),
    IN columnName VARCHAR(64),
    IN columnDefinitionType VARCHAR(64),
    IN columnDefinitionDefaultValue VARCHAR(128),
    IN columnDefinitionComment VARCHAR(255),
    IN afterColumnName VARCHAR(64)
)
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = tableName
          AND COLUMN_NAME = columnName
    ) THEN
        IF afterColumnName IS NOT NULL THEN
            SET @sql = CONCAT(
                'ALTER TABLE ', tableName,
                ' ADD COLUMN ', columnName, ' ', columnDefinitionType,
                ' DEFAULT ', columnDefinitionDefaultValue,
                ' COMMENT ''', columnDefinitionComment, '''',
                ' AFTER ', afterColumnName
            );
        ELSE
            SET @sql = CONCAT(
                'ALTER TABLE ', tableName,
                ' ADD COLUMN ', columnName, ' ', columnDefinitionType,
                ' DEFAULT ', columnDefinitionDefaultValue,
                ' COMMENT ''', columnDefinitionComment, ''''
            );
        END IF;
        PREPARE stmt FROM @sql;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;
    END IF;
END$$
DELIMITER ;

CALL add_column_if_not_exists(
    'dinky_task',
    'monitor_scan_status',
    'varchar(32)',
    '''NONE''',
    'Kubernetes 监控重扫状态：NONE 未触发，SCANNING 扫描中，SUCCESS 已恢复，FAILED 扫描失败',
    'job_instance_id'
);
