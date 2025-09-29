package org.andreaesposito.pgmq.jdbc.client;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;

public record MetricResult(String queueName, long queueLength, int newestMsgAgeSec, int oldestMsgAgeSec, long totalMessages, OffsetDateTime scrapeTime, long queueVisibleLength) {

	public static List<MetricResult> extract(ResultSet rs) throws SQLException {
		List<MetricResult> results = new ArrayList<>();
		while (rs.next()) {
			results.add(new MetricResult(rs.getString("queue_name"), rs.getLong("queue_length"), rs.getInt("newest_msg_age_sec"), rs.getInt("oldest_msg_age_sec"), rs.getLong("total_messages"),
					rs.getObject("scrape_time", OffsetDateTime.class), rs.getLong("queue_visible_length")));
		}
		return results;
	}
}
