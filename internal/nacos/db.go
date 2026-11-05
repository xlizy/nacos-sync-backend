package nacos

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

// OpenGroupDB 按分组配置的 DSN 打开该 Nacos 后端 MySQL 连接（仅用于轮询 config_info 变化）。
// 连接数压到最小（轮询很轻）；连接生命周期由上层 poller 在停止时显式 Close。
func OpenGroupDB(dsn string) (*sql.DB, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("open group db: %w", err)
	}
	db.SetMaxOpenConns(2)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(0)
	return db, nil
}

// 指纹计算 SQL：取 config_info 与 config_info_gray 两表的 id/tenant_id/md5，
// 拼成 "id|tenant_id|md5"，在 MySQL 内按 (src,id) 定序后 GROUP_CONCAT 成一长串再 MD5() 整体摘要。
// config_info_gray 仅 3.x 有，表不存在（1146）时改用 digestSQLMainOnly 降级。
// COALESCE 兜底：两表皆空（或删掉最后一条配置）时 GROUP_CONCAT 返回 NULL，MD5(NULL)=NULL，
// Scan 到 string 会报错、指纹永远算不出来——空库/清库后必须仍能得到稳定非空指纹，
// 「有 → 空」的变化才能被检出（空库指纹恒为 MD5(”)）。
const setGroupConcatMaxLen = "SET SESSION group_concat_max_len = 18446744073709551615"

const digestSQLBoth = `
SELECT MD5(COALESCE(GROUP_CONCAT(concat_str ORDER BY src, id), '')) AS total_md5
FROM (
  SELECT id, 'c' AS src, CONCAT(id, '|', IFNULL(tenant_id, ''), '|', md5) AS concat_str
  FROM config_info
  UNION ALL
  SELECT id, 'g' AS src, CONCAT(id, '|', IFNULL(tenant_id, ''), '|', md5)
  FROM config_info_gray
) t`

const digestSQLMainOnly = `
SELECT MD5(COALESCE(GROUP_CONCAT(concat_str ORDER BY id), '')) AS total_md5
FROM (
  SELECT id, CONCAT(id, '|', IFNULL(tenant_id, ''), '|', md5) AS concat_str
  FROM config_info
) t`

// ConfigDigest 计算该 Nacos 库「配置整体」的指纹，用于检测任意配置变化。
//
// 实现改为：在 MySQL 内用 GROUP_CONCAT(concat_str) + MD5() 直接算出整体指纹（比逐行拉回 Go 拼接更高效）。
// 为防止配置数多时 GROUP_CONCAT 受 group_concat_max_len（默认 1024）截断导致指纹失真、漏检变化，
// 先在同一连接上 SET SESSION group_concat_max_len 为最大值（用 sql.Conn 保证 SET 与 SELECT 落在同连接）。
// config_info_gray 仅 3.x 有；表不存在（错误号 1146）时降级为仅比对 config_info。
func ConfigDigest(ctx context.Context, db *sql.DB) (string, error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return "", fmt.Errorf("get conn: %w", err)
	}
	defer conn.Close()

	if _, err := conn.ExecContext(ctx, setGroupConcatMaxLen); err != nil {
		return "", fmt.Errorf("set group_concat_max_len: %w", err)
	}

	var digest string
	if err := conn.QueryRowContext(ctx, digestSQLBoth).Scan(&digest); err != nil {
		if isTableNotExist(err) {
			// 灰度表不存在：降级为仅比对主表
			if err2 := conn.QueryRowContext(ctx, digestSQLMainOnly).Scan(&digest); err2 != nil {
				return "", fmt.Errorf("compute md5 digest (degraded): %w", err2)
			}
			return digest, nil
		}
		return "", fmt.Errorf("compute md5 digest: %w", err)
	}
	return digest, nil
}

// isTableNotExist 判断是否为「表不存在」错误（MySQL 错误号 1146 / SQLState 42S02）。
func isTableNotExist(err error) bool {
	return err != nil && strings.Contains(err.Error(), "1146")
}
