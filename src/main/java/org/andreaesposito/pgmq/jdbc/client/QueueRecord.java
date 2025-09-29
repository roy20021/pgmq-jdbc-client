package org.andreaesposito.pgmq.jdbc.client;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;

public record QueueRecord(String queueName, boolean isPartitioned, boolean isUnlogged, OffsetDateTime createdAt) {

	public static List<QueueRecord> extract(ResultSet rs) throws SQLException {
		List<QueueRecord> results = new ArrayList<>();
		while (rs.next()) {
			results.add(new QueueRecord(rs.getString("queue_name"), rs.getBoolean("is_partitioned"), rs.getBoolean("is_unlogged"), rs.getObject("created_at", OffsetDateTime.class)));
		}
		return results;
	}
}
