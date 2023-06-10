// sql.go -- snippets for yaqpg, all in one place.
//
// Remember the any() trick!
//
//	https://github.com/jackc/pgx/issues/334
//	https://www.postgresql.org/docs/current/functions-comparisons.html#AEN21108

package yaqpg

import (
	"fmt"
)

func tableSQL(q *Queue, sqlf string) string {
	return fmt.Sprintf(sqlf, q.TableName)
}

func insertQueueItemSQL(q *Queue) string {
	return tableSQL(q, `-- insertQueueItemSQL
INSERT INTO %s
(queue_name, ident, payload, ready_at)
VALUES
($1,$2,$3,$4);`)
}

func selectCheckoutSQL(q *Queue) string {
	return tableSQL(q, `-- selectCheckoutSQL
SELECT id, ident, attempts, payload
FROM %s
WHERE queue_name = $1 AND ready_at <= now()
ORDER BY id ASC LIMIT $2
FOR UPDATE SKIP LOCKED;`)
}

func selectDeleteByIdListSQL(q *Queue) string {
	return tableSQL(q, `-- selectDeleteByIdListSQL
DELETE FROM %s
WHERE id = ANY($1);`)
}

func updateReadyAtByIdListSQL(q *Queue) string {
	return tableSQL(q, `-- updateReadyAtByIdListSQL
UPDATE %s
SET ready_at = $1, attempts = attempts + 1
WHERE id = ANY($2);`)
}

func selectCountSQL(q *Queue) string {
	return tableSQL(q, `-- selectCountSQL
SELECT COUNT(*)
FROM %s
WHERE queue_name = $1;`)
}

func selectCountReadySQL(q *Queue) string {
	return tableSQL(q, `-- selectCountReadySQL
SELECT COUNT(*)
FROM %s
WHERE queue_name = $1 AND ready_at <= now();`)
}

func selectCountPendingSQL(q *Queue) string {
	return tableSQL(q, `-- selectCountPendingSQL
SELECT COUNT(*)
FROM %s
WHERE queue_name = $1 AND ready_at > now();`)
}

func insertPlaceholderSQL(q *Queue) string {
	return tableSQL(q, `-- insertPlaceholderSQL
INSERT INTO %s
(queue_name, ident, payload, ready_at)
VALUES
($1,$2,$3,$4);`)
}

func deletePlaceholderSQL(q *Queue) string {
	return tableSQL(q, `-- deletePlaceholderSQL
DELETE FROM %s
WHERE ident = $1;`)
}

// Schema returns the SQL statements needed to (re)create the database table
// for this queue.
func Schema(q *Queue) string {
	// multiple replacements here, so...
	sqlf := `-- schemaSQL: create the YAQPg schema for queue %s
	DROP TABLE IF EXISTS %s;
CREATE TABLE %s (
	id BIGSERIAL NOT NULL PRIMARY KEY,
	queue_name TEXT NOT NULL,
	ident TEXT NOT NULL,
	ready_at TIMESTAMP NOT NULL,
	attempts INT NOT NULL DEFAULT 0,
	payload BYTEA NOT NULL DEFAULT BYTEA('')
);
CREATE UNIQUE INDEX %s_nit_idx ON %s USING BTREE(queue_name,ident,ready_at);
`
	return fmt.Sprintf(sqlf,
		q.Name,
		q.TableName,
		q.TableName,
		q.TableName,
	)
}
