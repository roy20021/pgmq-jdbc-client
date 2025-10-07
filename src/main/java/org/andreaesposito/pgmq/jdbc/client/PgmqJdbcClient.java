package org.andreaesposito.pgmq.jdbc.client;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLTransientException;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import org.postgresql.core.Oid;
import org.postgresql.util.PGobject;

/**
 * A JDBC-only client for the PGMQ Extension
 */
public class PgmqJdbcClient {

	private final int queryTimeoutSeconds;
	private final int maxRetries;

	private final Connection connection;

	public PgmqJdbcClient(Connection connection) {
		this(connection, 0, 0);
	}

	public PgmqJdbcClient(Connection connection, int queryTimeoutSeconds, int maxRetries) {
		this.connection = Objects.requireNonNull(connection, "Connection must not be null");
		if (queryTimeoutSeconds < 0)
			throw new IllegalArgumentException("timeout must be >= 0");
		if (maxRetries < 0)
			throw new IllegalArgumentException("maxRetries must be >= 0");
		this.queryTimeoutSeconds = queryTimeoutSeconds;
		this.maxRetries = maxRetries;
	}

	/* Sending Messages */
	public List<Long> send(String queueName, Json msg) throws SQLException {
		String sql = "SELECT pgmq.send(queue_name => ?, msg => ?)";
		return executeQuery(LONG_EXTRACTOR, sql, queueName, msg);
	}

	public List<Long> send(String queueName, Json msg, int delay) throws SQLException {
		String sql = "SELECT pgmq.send(queue_name => ?, msg => ?, delay => ?)";
		return executeQuery(LONG_EXTRACTOR, sql, queueName, msg, delay);
	}

	public List<Long> send(String queueName, Json msg, OffsetDateTime delay) throws SQLException {
		String sql = "SELECT pgmq.send(queue_name => ?, msg => ?, delay => ?)";
		return executeQuery(LONG_EXTRACTOR, sql, queueName, msg, delay);
	}

	public List<Long> send(String queueName, Json msg, Json headers) throws SQLException {
		String sql = "SELECT pgmq.send(queue_name => ?, msg => ?, headers => ?)";
		return executeQuery(LONG_EXTRACTOR, sql, queueName, msg, headers);
	}

	public List<Long> send(String queueName, Json msg, Json headers, int delay) throws SQLException {
		String sql = "SELECT pgmq.send(queue_name => ?, msg => ?, headers => ?, delay => ?)";
		return executeQuery(LONG_EXTRACTOR, sql, queueName, msg, headers, delay);
	}

	public List<Long> send(String queueName, Json msg, Json headers, OffsetDateTime delay) throws SQLException {
		String sql = "SELECT pgmq.send(queue_name => ?, msg => ?, headers => ?, delay => ?)";
		return executeQuery(LONG_EXTRACTOR, sql, queueName, msg, headers, delay);
	}

	public List<Long> sendBatch(String queueName, Json[] msgs) throws SQLException {
		String sql = "SELECT pgmq.send_batch(queue_name => ?, msgs => ?)";
		return executeQuery(LONG_EXTRACTOR, sql, queueName, msgs);
	}

	public List<Long> sendBatch(String queueName, Json[] msgs, int delay) throws SQLException {
		String sql = "SELECT pgmq.send(queue_name => ?, msgs => ?, delay => ?)";
		return executeQuery(LONG_EXTRACTOR, sql, queueName, msgs, delay);
	}

	public List<Long> sendBatch(String queueName, Json[] msgs, OffsetDateTime delay) throws SQLException {
		String sql = "SELECT pgmq.send_batch(queue_name => ?, msgs => ?, delay => ?)";
		return executeQuery(LONG_EXTRACTOR, sql, queueName, msgs, delay);
	}

	public List<Long> sendBatch(String queueName, Json[] msgs, Json[] headers) throws SQLException {
		if (msgs.length != headers.length)
			throw new IllegalArgumentException("msgs and headers must be the same length");
		String sql = "SELECT pgmq.send_batch(queue_name => ?, msgs => ?, headers => ?)";
		return executeQuery(LONG_EXTRACTOR, sql, queueName, msgs, headers);
	}

	public List<Long> sendBatch(String queueName, Json[] msgs, Json[] headers, int delay) throws SQLException {
		if (msgs.length != headers.length)
			throw new IllegalArgumentException("msgs and headers must be the same length");
		String sql = "SELECT pgmq.send_batch(queue_name => ?, msgs => ?, headers => ?, delay => ?)";
		return executeQuery(LONG_EXTRACTOR, sql, queueName, msgs, headers, delay);
	}

	public List<Long> sendBatch(String queueName, Json[] msgs, Json[] headers, OffsetDateTime delay) throws SQLException {
		if (msgs.length != headers.length)
			throw new IllegalArgumentException("msgs and headers must be the same length");
		String sql = "SELECT pgmq.send_batch(queue_name => ?, msgs => ?, headers => ?, delay => ?)";
		return executeQuery(LONG_EXTRACTOR, sql, queueName, msgs, headers, delay);
	}

	/* Reading Messages */
	public List<MessageRecord> read(String queueName, int visibilityTimeoutSeconds, int qty, Json conditional) throws SQLException {
		String sql = "SELECT msg_id, read_ct, enqueued_at, vt, headers, message FROM pgmq.read(queue_name => ?, vt => ?, qty => ?, conditional => ?)";
		return executeQuery(MessageRecord::extract, sql, queueName, visibilityTimeoutSeconds, qty, conditional);
	}

	public List<MessageRecord> readWithPoll(String queueName, int visibilityTimeoutSeconds, int qty, int maxPollSeconds, int pollIntervalMs, Json conditional) throws SQLException {
		String sql = "SELECT msg_id, read_ct, enqueued_at, vt, headers, message FROM pgmq.read_with_poll(queue_name => ?, vt => ?, qty => ?, max_poll_seconds => ?, poll_interval_ms => ?, conditional => ?)";
		return executeQuery(MessageRecord::extract, sql, queueName, visibilityTimeoutSeconds, qty, maxPollSeconds, pollIntervalMs, conditional);
	}

	public List<MessageRecord> readWithPoll(String queueName, int visibilityTimeoutSeconds, int qty, Json conditional) throws SQLException {
		String sql = "SELECT msg_id, read_ct, enqueued_at, vt, headers, message FROM pgmq.read_with_poll(queue_name => ?, vt => ?, qty => ?, conditional => ?)";
		return executeQuery(MessageRecord::extract, sql, queueName, visibilityTimeoutSeconds, qty, conditional);
	}

	public Optional<MessageRecord> pop(String queueName) throws SQLException {
		String sql = "SELECT msg_id, read_ct, enqueued_at, vt, headers, message FROM pgmq.pop(queue_name => ?)";
		return executeQuery(MessageRecord::extract, sql, queueName).stream().findFirst();
	}

	/* Deleting/Archiving Messages */
	public boolean delete(String queueName, long msgId) throws SQLException {
		String sql = "SELECT pgmq.delete(queue_name => ?, msg_id => ?)";
		return executeQuery(BOOLEAN_EXTRACTOR, sql, queueName, msgId).getFirst();
	}

	public List<Long> delete(String queueName, long[] msgId) throws SQLException {
		String sql = "SELECT pgmq.delete(queue_name => ?, msg_ids => ?::bigint[])";
		return executeQuery(LONG_EXTRACTOR, sql, queueName, msgId);
	}

	public Long purge(String queueName) throws SQLException {
		String sql = "SELECT pgmq.purge_queue(queue_name => ?)";
		return executeQuery(LONG_EXTRACTOR, sql, queueName).getFirst();
	}

	public boolean archive(String queueName, long msgId) throws SQLException {
		String sql = "SELECT pgmq.archive(queue_name => ?, msg_id => ?)";
		return executeQuery(BOOLEAN_EXTRACTOR, sql, queueName, msgId).getFirst();
	}

	public List<Long> archive(String queueName, long[] msgIds) throws SQLException {
		String sql = "SELECT * FROM pgmq.archive(queue_name => ?, msg_ids => ?::bigint[])";
		return executeQuery(LONG_EXTRACTOR, sql, queueName, msgIds);
	}

	/* Queue Management */
	public void create(String queueName) throws SQLException {
		String sql = "SELECT pgmq.create(queue_name => ?)";
		executeQuery(sql, queueName);
	}

	public void createUnlogged(String queueName) throws SQLException {
		String sql = "SELECT pgmq.create_unlogged(queue_name => ?)";
		executeQuery(sql, queueName);
	}

	public boolean drop(String queueName) throws SQLException {
		String sql = "SELECT pgmq.drop_queue(queue_name => ?)";
		return executeQuery(BOOLEAN_EXTRACTOR, sql, queueName).getFirst();
	}

	/* Utilities */
	public List<QueueRecord> listQueues() throws SQLException {
		String sql = "SELECT * FROM pgmq.list_queues()";
		return executeQuery(QueueRecord::extract, sql);
	}

	public MetricResult metrics(String queueName) throws SQLException {
		String sql = "SELECT * FROM pgmq.metrics(queue_name => ?)";
		return executeQuery(MetricResult::extract, sql, queueName).getFirst();
	}

	public List<MetricResult> metricsAll() throws SQLException {
		String sql = "SELECT * FROM pgmq.metrics_all()";
		return executeQuery(MetricResult::extract, sql);
	}

	public Optional<MessageRecord> setVisibilityTimeout(String queueName, long msgId, int visibilityTimeoutSeconds) throws SQLException {
		String sql = "SELECT msg_id, read_ct, enqueued_at, vt, headers, message FROM pgmq.set_vt(queue_name => ?, msg_id => ?, vt => ?)";
		return executeQuery(MessageRecord::extract, sql, queueName, msgId, visibilityTimeoutSeconds).stream().findFirst();
	}

	/* Client's internals */
	private static interface SqlSupplier<T> {
		T get() throws SQLException;
	}

	private static interface SqlExtractor<T> {
		List<T> extract(ResultSet rs) throws SQLException;
	}

	private static final SqlExtractor<Long> LONG_EXTRACTOR = rs -> {
		List<Long> results = new ArrayList<>();
		while (rs.next()) {
			results.add(rs.getLong(1));
		}
		return results;
	};

	private static final SqlExtractor<Boolean> BOOLEAN_EXTRACTOR = rs -> {
		List<Boolean> results = new ArrayList<>();
		while (rs.next()) {
			results.add(rs.getBoolean(1));
		}
		return results;
	};

	private <T> List<T> executeQuery(SqlExtractor<T> extractor, String sqlTemplate, Object... params) throws SQLException {
		return retrying(() -> {
			try (PreparedStatement ps = connection.prepareStatement(sqlTemplate)) {
				ps.setQueryTimeout(queryTimeoutSeconds);
				for (int i = 0; i < params.length; i++) {
					if (params[i] instanceof Json(String value)) {
						PGobject pgObject = new PGobject();
						pgObject.setType(Oid.toString(Oid.JSONB));
						pgObject.setValue(value);
						ps.setObject(i + 1, pgObject);
					} else if (params[i] instanceof Json[] jsons) {
						String[] values = Arrays.stream(jsons).map(Json::value).toArray(String[]::new);
						ps.setArray(i + 1, connection.createArrayOf(Oid.toString(Oid.JSONB), values));
					} else if (params[i] instanceof String text) {
						PGobject pgObject = new PGobject();
						pgObject.setType(Oid.toString(Oid.TEXT));
						pgObject.setValue(text);
						ps.setObject(i + 1, pgObject);
					} else {
						ps.setObject(i + 1, params[i]);
					}
				}
				try (ResultSet rs = ps.executeQuery()) {
					return extractor.extract(rs);
				}
			}
		});
	}

	private void executeQuery(String sqlTemplate, Object... params) throws SQLException {
		executeQuery(rs -> null, sqlTemplate, params);
	}

	private <T> T retrying(SqlSupplier<T> fn) throws SQLException {
		int tryCount = 0;
		while (true) {
			try {
				return fn.get();
			} catch (SQLTransientException transientEx) {
				if (tryCount >= maxRetries)
					throw transientEx;
				tryCount++;
			}
		}
	}
}
