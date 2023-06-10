-- default schema for pgslq
DROP TABLE IF EXISTS yaqpg_queue;
CREATE TABLE yaqpg_queue (
	id BIGSERIAL NOT NULL PRIMARY KEY,
	queue_name TEXT NOT NULL,
	ident TEXT NOT NULL,
	ready_at TIMESTAMP NOT NULL,
	attempts INT NOT NULL DEFAULT 0,
	payload BYTEA NOT NULL DEFAULT BYTEA('')
);
CREATE UNIQUE INDEX yaqpg_queue_nit_idx ON yaqpg_queue USING BTREE(queue_name,ident,ready_at);

