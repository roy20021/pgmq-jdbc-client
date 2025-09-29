package org.andreaesposito.pgmq.jdbc.client;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;

public record MessageRecord(long msgId, int readCt, OffsetDateTime enqueuedAt, OffsetDateTime visibilityTimeout, Json headers, Json message) {

	public static List<MessageRecord> extract(ResultSet rs) throws SQLException {
		List<MessageRecord> results = new ArrayList<>();
		while (rs.next()) {
			results.add(new MessageRecord(rs.getLong("msg_id"), rs.getInt("read_ct"), rs.getObject("enqueued_at", OffsetDateTime.class), rs.getObject("vt", OffsetDateTime.class), new Json(rs.getString("headers")),
					new Json(rs.getString("message"))));
		}
		return results;
	}
}
