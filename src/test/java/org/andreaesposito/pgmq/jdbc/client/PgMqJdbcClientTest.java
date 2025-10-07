package org.andreaesposito.pgmq.jdbc.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.SQLException;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class PgMqJdbcClientTest {

	@SuppressWarnings("resource")
	@Container
	public static GenericContainer<?> database = new GenericContainer<>(DockerImageName.parse("ghcr.io/pgmq/pg17-pgmq:v1.6.1")).withEnv("POSTGRES_PASSWORD", "postgres").withExposedPorts(5432);

	private static PgmqJdbcClient client;
	private static final String QUEUE = "test_queue";

	@BeforeAll
	static void setUpAll() throws SQLException {
		PGSimpleDataSource ds = new PGSimpleDataSource();
		ds.setServerNames(new String[] { database.getHost() });
		ds.setUser("postgres");
		ds.setPassword("postgres");
		ds.setPortNumbers(new int[] { database.getFirstMappedPort() });

		ds.getConnection().createStatement().execute("CREATE EXTENSION IF NOT EXISTS pgmq");

		client = new PgmqJdbcClient(ds.getConnection());
	}

	@BeforeEach
	void tearUp() throws SQLException {
		client.create(QUEUE);
	}

	@AfterEach
	void tearDownAll() throws SQLException {
		client.drop(QUEUE);
	}

	@Test
	@Order(1)
	void testSendAndRead() throws SQLException {
		Json msg = new Json("{\"hello\":\"world\"}");
		List<Long> ids = client.send(QUEUE, msg);
		assertEquals(1, ids.size());

		List<MessageRecord> read = client.read(QUEUE, 10, 1, null);
		assertEquals(1, read.size());
		assertEquals(ids.get(0), read.get(0).msgId());
	}

	@Test
	@Order(2)
	void testSendWithDelay() throws SQLException {
		Json msg = new Json("{\"delayed\":\"yes\"}");
		List<Long> ids = client.send(QUEUE, msg, 5);
		assertEquals(1, ids.size());
	}

	@Test
	@Order(3)
	void testSendWithHeaders() throws SQLException {
		Json msg = new Json("{\"foo\":\"bar\"}");
		Json headers = new Json("{\"hdr\":\"val\"}");
		List<Long> ids = client.send(QUEUE, msg, headers);
		assertEquals(1, ids.size());
	}

	@Test
	@Order(4)
	void testSendBatchAndHeaders() throws SQLException {
		Json[] msgs = { new Json("{\"m1\":1}"), new Json("{\"m2\":2}") };
		Json[] headers = { new Json("{\"h1\":1}"), new Json("{\"h2\":2}") };
		List<Long> ids = client.sendBatch(QUEUE, msgs, headers);
		assertEquals(2, ids.size());
	}

	@Test
	@Order(5)
	void testSendBatchMismatchedHeadersThrows() {
		Json[] msgs = { new Json("{\"m1\":1}") };
		Json[] headers = { new Json("{\"h1\":1}"), new Json("{\"h2\":2}") };
		assertThrows(IllegalArgumentException.class, () -> client.sendBatch(QUEUE, msgs, headers));
	}

	@Test
	@Order(6)
	void testPop() throws SQLException {
		Json msg = new Json("{\"pop\":\"me\"}");
		Long id = client.send(QUEUE, msg).getFirst();

		Optional<MessageRecord> popped = client.pop(QUEUE);
		assertTrue(popped.isPresent());
		assertEquals(id, popped.get().msgId());
	}

	@Test
	@Order(7)
	void testDeleteBatch() throws SQLException {
		Json msg = new Json("{\"del\":\"multi\"}");
		List<Long> ids = client.sendBatch(QUEUE, new Json[] { msg, msg });
		List<Long> deleted = client.delete(QUEUE, ids.stream().mapToLong(Long::longValue).toArray());
		assertEquals(ids.size(), deleted.size());
	}

	@Test
	@Order(8)
	void testArchiveAndPurge() throws SQLException {
		Long id = client.send(QUEUE, new Json("{\"arch\":\"me\"}")).getFirst();
		boolean archived = client.archive(QUEUE, id);
		assertTrue(archived);

		Long purged = client.purge(QUEUE);
		assertEquals(0, purged);
	}

	@Test
	@Order(9)
	void testQueueManagement() throws SQLException {
		String tempQueue = "temp_queue";
		client.createUnlogged(tempQueue);
		assertTrue(client.drop(tempQueue));
	}

	@Test
	@Order(10)
	void testListQueuesAndMetrics() throws SQLException {
		List<QueueRecord> queues = client.listQueues();
		assertFalse(queues.isEmpty());

		MetricResult m = client.metrics(QUEUE);
		assertNotNull(m);

		List<MetricResult> all = client.metricsAll();
		assertFalse(all.isEmpty());
	}

	@Test
	@Order(11)
	void testSetVisibilityTimeout() throws SQLException {
		Long id = client.send(QUEUE, new Json("{\"vt\":\"test\"}")).getFirst();
		Optional<MessageRecord> rec = client.setVisibilityTimeout(QUEUE, id, 30);
		assertTrue(rec.isPresent());
		assertEquals(id, rec.get().msgId());
	}

	@Test
	@Order(12)
	void testReadWithPoll() throws SQLException {
		Long id = client.send(QUEUE, new Json("{\"poll\":\"test\"}"), 2).getFirst();
		List<MessageRecord> recs = client.readWithPoll(QUEUE, 10, 4, null);
		assertFalse(recs.isEmpty());
		assertEquals(id, recs.get(0).msgId());
	}

	@Test
	@Order(13)
	void testReadWithPollAdvanced() throws SQLException {
		Long id = client.send(QUEUE, new Json("{\"poll2\":\"test\"}"), 1).getFirst();
		List<MessageRecord> recs = client.readWithPoll(QUEUE, 10, 1, 4, 100, null);
		assertFalse(recs.isEmpty());
		assertEquals(id, recs.get(0).msgId());
	}
}
