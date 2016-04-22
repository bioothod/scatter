#include "scatter/node.hpp"
#include "scatter/server.hpp"

#include <gtest/gtest.h>

using namespace ioremap::scatter;

class stest : public ::testing::Test {
protected:
	stest()
	: m_addr1("127.0.0.1:21001:2")
	, m_addr2("127.0.0.1:21002:2")
	, m_s1(m_addr1, 4)
	, m_s2(m_addr2, 4)
	{
		LOG(INFO) << "New test starts";
	}

	virtual ~stest() {
	}

	void set_ids(std::vector<connection::cid_t> &ids1, std::vector<connection::cid_t> &ids2) {
		m_s1.test_set_ids(ids1);
		m_s2.test_set_ids(ids2);
	}

	void connect_servers() {
		connection::pointer cn = m_s2.connect(m_addr1);
		m_s2.join(cn);
	}

	void s2_client_process(connection::pointer cn, message &msg) {
	}

	std::string m_addr1, m_addr2;
	server m_s1, m_s2;
};

TEST_F(stest, route)
{
	std::vector<connection::cid_t> ids1 = {0, 500};
	std::vector<connection::cid_t> ids2 = {100, 600};

	set_ids(ids1, ids2);
	connect_servers();

	node c;

	c.connect(m_addr1, [&] (connection::pointer, message &) {});
	auto cn = c.get_connection(90);
	ASSERT_NE(cn.use_count(), 0);
	ASSERT_EQ(cn->remote_string(), m_addr1);

	cn = c.get_connection(123);
	ASSERT_NE(cn.use_count(), 0);
	ASSERT_EQ(cn->remote_string(), m_addr2);

	cn = c.get_connection(1000000);
	ASSERT_NE(cn.use_count(), 0);
	ASSERT_EQ(cn->remote_string(), m_addr2);

	c.connect(m_addr2, [&] (connection::pointer, message &) {});
	cn = c.get_connection(123);
	ASSERT_NE(cn.use_count(), 0);
	ASSERT_EQ(cn->remote_string(), m_addr2);

	cn = c.get_connection(1000000);
	ASSERT_NE(cn.use_count(), 0);
	ASSERT_EQ(cn->remote_string(), m_addr2);

	return;
}

TEST_F(stest, bcast_one_to_one)
{
	std::vector<connection::cid_t> ids1 = {0, 500};
	std::vector<connection::cid_t> ids2 = {100, 600};

	set_ids(ids1, ids2);

	node c1, c2, c3;
	uint64_t db = 123;

	std::mutex lock;
	std::condition_variable cv;

	std::atomic_ulong c1_counter(0), c1_completed(0);
	c1.connect(m_addr1, [&] (connection::pointer, message &) {
					c1_counter++;
			});
	c1.bcast_join(db);

	std::atomic_ulong c2_counter(0);
	c2.connect(m_addr1, [&] (connection::pointer, message &) {
					c2_counter++;
			});
	c2.bcast_join(db);

	uint64_t c1_bias = 10000;

	uint64_t n = 10000;
	for (uint64_t i = 0; i < n; ++i) {
		message msg;
		msg.hdr.id = c1_bias + i;
		msg.hdr.db = db;
		msg.hdr.cmd = SCATTER_CMD_CLIENT + 1;
		msg.hdr.flags = SCATTER_FLAGS_NEED_ACK;
		msg.encode_header();

		c1.send(msg, [&] (connection::pointer, message &reply) {
					ASSERT_EQ(reply.hdr.id, c1_bias + c1_completed.load());
					c1_completed++;
					cv.notify_one();
				});
	}

	std::unique_lock<std::mutex> l(lock);
	cv.wait_for(l, std::chrono::seconds(10), [&] {return c1_completed == n;});
	l.unlock();

	ASSERT_EQ(c1_completed, n);
	ASSERT_EQ(c1_completed, c2_counter);

	return;
}

TEST_F(stest, bcast_one_to_one_bidirectional)
{
	std::vector<connection::cid_t> ids1 = {0, 500};
	std::vector<connection::cid_t> ids2 = {100, 600};

	set_ids(ids1, ids2);

	node c1, c2, c3;
	uint64_t db = 123;

	std::mutex lock;
	std::condition_variable cv1, cv2;

	std::atomic_ulong c1_counter(0), c1_completed(0);
	c1.connect(m_addr1, [&] (connection::pointer, message &) {
					c1_counter++;
			});
	c1.bcast_join(db);

	std::atomic_ulong c2_counter(0), c2_completed(0);
	c2.connect(m_addr1, [&] (connection::pointer, message &) {
					c2_counter++;
			});
	c2.bcast_join(db);

	uint64_t c1_bias = 10000;
	uint64_t c2_bias = 30000;

	uint64_t n = 10000;
	for (uint64_t i = 0; i < n; ++i) {
		message msg;
		msg.hdr.id = c1_bias + i;
		msg.hdr.db = db;
		msg.hdr.cmd = SCATTER_CMD_CLIENT + 1;
		msg.hdr.flags = SCATTER_FLAGS_NEED_ACK;
		msg.encode_header();

		c1.send(msg, [&] (connection::pointer, message &reply) {
					ASSERT_EQ(reply.hdr.id, c1_bias + c1_completed.load());
					c1_completed++;
					cv1.notify_one();
				});

		message m2;
		m2.hdr.id = c2_bias + i;
		m2.hdr.db = db;
		m2.hdr.cmd = SCATTER_CMD_CLIENT + 1;
		m2.hdr.flags = SCATTER_FLAGS_NEED_ACK;
		m2.encode_header();
		c2.send(m2, [&] (connection::pointer, message &reply) {
					ASSERT_EQ(reply.hdr.id, c2_bias + c2_completed.load());
					c2_completed++;
					cv2.notify_one();
				});
	}

	std::unique_lock<std::mutex> l1(lock);
	cv1.wait_for(l1, std::chrono::seconds(10), [&] {return c1_completed == n;});
	l1.unlock();

	ASSERT_EQ(c1_completed, n);
	ASSERT_EQ(c1_completed, c2_counter);

	std::unique_lock<std::mutex> l2(lock);
	cv2.wait_for(l2, std::chrono::seconds(10), [&] {return c2_completed == n;});
	l2.unlock();

	ASSERT_EQ(c2_completed, n);
	ASSERT_EQ(c2_completed, c1_counter);

	return;
}


TEST_F(stest, bcast_one_to_many)
{
	std::vector<connection::cid_t> ids1 = {0, 500};
	std::vector<connection::cid_t> ids2 = {100, 600};

	set_ids(ids1, ids2);

	node c1, c2, c3;
	uint64_t db = 123;

	std::mutex lock;
	std::condition_variable cv1, cv2;

	std::atomic_ulong c1_counter(0), c1_completed(0);
	c1.connect(m_addr1, [&] (connection::pointer, message &) {
					c1_counter++;
			});
	c1.bcast_join(db);

	std::atomic_ulong c2_counter(0), c2_completed(0);
	c2.connect(m_addr1, [&] (connection::pointer, message &) {
					c2_counter++;
			});
	c2.bcast_join(db);

	std::atomic_ulong c3_counter(0), c3_completed(0);
	c3.connect(m_addr1, [&] (connection::pointer, message &) {
					c3_counter++;
			});
	c3.bcast_join(db);

	uint64_t c1_bias = 10000;

	uint64_t n = 10000;
	for (uint64_t i = 0; i < n; ++i) {
		message msg;
		msg.hdr.id = c1_bias + i;
		msg.hdr.db = db;
		msg.hdr.cmd = SCATTER_CMD_CLIENT + 1;
		msg.hdr.flags = SCATTER_FLAGS_NEED_ACK;
		msg.encode_header();

		c1.send(msg, [&] (connection::pointer, message &reply) {
					ASSERT_EQ(reply.hdr.id, c1_bias + c1_completed.load());
					c1_completed++;
					cv1.notify_one();
				});
	}

	std::unique_lock<std::mutex> l1(lock);
	cv1.wait_for(l1, std::chrono::seconds(10), [&] {return c1_completed == n;});
	l1.unlock();

	ASSERT_EQ(c1_completed, n);
	ASSERT_EQ(c1_completed, c2_counter);
	ASSERT_EQ(c1_completed, c3_counter);

	return;
}

TEST_F(stest, bcast_many_to_many)
{
	std::vector<connection::cid_t> ids1 = {0, 500};
	std::vector<connection::cid_t> ids2 = {100, 600};

	set_ids(ids1, ids2);

	node c1, c2, c3;
	uint64_t db = 123;

	std::mutex lock;
	std::condition_variable cv1, cv2;

	std::atomic_ulong c1_counter(0), c1_completed(0);
	c1.connect(m_addr1, [&] (connection::pointer, message &) {
					c1_counter++;
			});
	c1.bcast_join(db);

	std::atomic_ulong c2_counter(0), c2_completed(0);
	c2.connect(m_addr1, [&] (connection::pointer, message &) {
					c2_counter++;
			});
	c2.bcast_join(db);

	std::atomic_ulong c3_counter(0), c3_completed(0);
	c3.connect(m_addr1, [&] (connection::pointer, message &) {
					c3_counter++;
			});
	c3.bcast_join(db);

	uint64_t c1_bias = 10000;
	uint64_t c2_bias = 30000;

	uint64_t n = 10000;
	for (uint64_t i = 0; i < n; ++i) {
		message msg;
		msg.hdr.id = c1_bias + i;
		msg.hdr.db = db;
		msg.hdr.cmd = SCATTER_CMD_CLIENT + 1;
		msg.hdr.flags = SCATTER_FLAGS_NEED_ACK;
		msg.encode_header();

		c1.send(msg, [&] (connection::pointer, message &reply) {
					ASSERT_EQ(reply.hdr.id, c1_bias + c1_completed.load());
					c1_completed++;
					cv1.notify_one();
				});
		message m2;
		m2.hdr.id = c2_bias + i;
		m2.hdr.db = db;
		m2.hdr.cmd = SCATTER_CMD_CLIENT + 1;
		m2.hdr.flags = SCATTER_FLAGS_NEED_ACK;
		m2.encode_header();
		c2.send(m2, [&] (connection::pointer, message &reply) {
					ASSERT_EQ(reply.hdr.id, c2_bias + c2_completed.load());
					c2_completed++;
					cv2.notify_one();
				});
	}

	std::unique_lock<std::mutex> l1(lock);
	cv1.wait_for(l1, std::chrono::seconds(10), [&] {return c1_completed == n;});
	l1.unlock();

	ASSERT_EQ(c1_completed, n);
	ASSERT_EQ(c1_completed, c2_counter);

	std::unique_lock<std::mutex> l2(lock);
	cv2.wait_for(l2, std::chrono::seconds(10), [&] {return c2_completed == n;});
	l2.unlock();

	ASSERT_EQ(c2_completed, n);
	ASSERT_EQ(c2_completed, c1_counter);

	ASSERT_EQ(c1_completed + c2_completed, c3_counter);

	return;
}

TEST_F(stest, route_table_update)
{
	std::vector<connection::cid_t> ids1 = {0, 500};
	std::vector<connection::cid_t> ids2 = {100, 600};

	set_ids(ids1, ids2);

	node c1, c2, c3;
	uint64_t db = 123;

	std::mutex lock;
	std::condition_variable cv1, cv2;

	std::atomic_ulong c1_counter(0), c1_completed(0);
	c1.connect(m_addr1, [&] (connection::pointer, message &) {
					c1_counter++;
			});
	c1.bcast_join(db);

	connect_servers();

	std::atomic_ulong c2_counter(0), c2_completed(0);
	c2.connect(m_addr1, [&] (connection::pointer, message &) {
					c2_counter++;
			});
	c2.bcast_join(db);

	std::atomic_ulong c3_counter(0), c3_completed(0);
	c3.connect(m_addr1, [&] (connection::pointer, message &) {
					c3_counter++;
			});
	c3.connect(m_addr2, [&] (connection::pointer, message &) {
					c3_counter++;
			});
	c3.bcast_join(db);
	auto cn = c3.get_connection(db);
	ASSERT_NE(cn.use_count(), 0);
	ASSERT_EQ(cn->remote_string(), m_addr2);

	uint64_t c1_bias = 10000;
	uint64_t c2_bias = 30000;

	uint64_t n = 10000;
	for (uint64_t i = 0; i < n; ++i) {
		message msg;
		msg.hdr.id = c1_bias + i;
		msg.hdr.db = db;
		msg.hdr.cmd = SCATTER_CMD_CLIENT + 1;
		msg.hdr.flags = SCATTER_FLAGS_NEED_ACK;
		msg.encode_header();

		c1.send(msg, [&] (connection::pointer, message &reply) {
					ASSERT_EQ(reply.hdr.id, c1_bias + c1_completed.load());
					c1_completed++;
					cv1.notify_one();
				});
		message m2;
		m2.hdr.id = c2_bias + i;
		m2.hdr.db = db;
		m2.hdr.cmd = SCATTER_CMD_CLIENT + 1;
		m2.hdr.flags = SCATTER_FLAGS_NEED_ACK;
		m2.encode_header();
		c2.send(m2, [&] (connection::pointer, message &reply) {
					ASSERT_EQ(reply.hdr.id, c2_bias + c2_completed.load());
					c2_completed++;
					cv2.notify_one();
				});
	}

	std::unique_lock<std::mutex> l1(lock);
	cv1.wait_for(l1, std::chrono::seconds(10), [&] {return c1_completed == n;});
	l1.unlock();

	ASSERT_EQ(c1_completed, n);
	ASSERT_EQ(c1_completed, c2_counter);

	std::unique_lock<std::mutex> l2(lock);
	cv2.wait_for(l2, std::chrono::seconds(10), [&] {return c2_completed == n;});
	l2.unlock();

	ASSERT_EQ(c2_completed, n);
	ASSERT_EQ(c2_completed, c1_counter);

	ASSERT_EQ(c1_completed + c2_completed, c3_counter);

	return;
}

TEST_F(stest, leave_broadcast_groups)
{
	std::vector<connection::cid_t> ids1 = {0, 500};
	std::vector<connection::cid_t> ids2 = {100, 600};

	set_ids(ids1, ids2);

	node c1, c2, c3;
	uint64_t db = 123;

	std::mutex lock;
	std::condition_variable cv1, cv2;

	// the first two clients will be connected to srv1
	// the third one will be connected to srv2
	// srv1 will add srv2 into local broadcast group
	c1.connect(m_addr1, [&] (connection::pointer, message &) {});
	c1.bcast_join(db);

	// srv2 connects to and joins srv1
	// srv1 receives command SCATTER_CMD_SERVER_JOIN from srv2
	// srv1 announces local broadcast groups by sending SCATTER_CMD_BCAST_JOIN command to this server-server connection
	// srv2 thus receives SCATTER_CMD_BCAST_JOIN command from srv1
	connect_servers();

	ASSERT_EQ(m_s1.test_bcast_num_connections(db, false), 1);
	ASSERT_EQ(m_s1.test_bcast_num_connections(db, true), 1);
	ASSERT_EQ(m_s2.test_bcast_num_connections(db, false), 1);
	ASSERT_EQ(m_s2.test_bcast_num_connections(db, true), 0);

	c2.connect(m_addr1, [&] (connection::pointer, message &) {});
	c2.bcast_join(db);
	ASSERT_EQ(m_s1.test_bcast_num_connections(db, false), 1);
	ASSERT_EQ(m_s1.test_bcast_num_connections(db, true), 1);
	ASSERT_EQ(m_s2.test_bcast_num_connections(db, false), 2);
	ASSERT_EQ(m_s2.test_bcast_num_connections(db, true), 0);

	c3.connect(m_addr1, [&] (connection::pointer, message &) {});
	c3.connect(m_addr2, [&] (connection::pointer, message &) {});
	c3.bcast_join(db);
	ASSERT_EQ(m_s1.test_bcast_num_connections(db, false), 1);
	ASSERT_EQ(m_s1.test_bcast_num_connections(db, true), 1);
	ASSERT_EQ(m_s2.test_bcast_num_connections(db, false), 3);
	ASSERT_EQ(m_s2.test_bcast_num_connections(db, true), 0);

	c1.bcast_leave(db);
	c2.bcast_leave(db);
	ASSERT_EQ(m_s1.test_bcast_num_connections(db, false), 0);
	ASSERT_EQ(m_s1.test_bcast_num_connections(db, true), 0);
	ASSERT_EQ(m_s2.test_bcast_num_connections(db, false), 1);
	ASSERT_EQ(m_s2.test_bcast_num_connections(db, true), 0);

	c3.bcast_leave(db);
	ASSERT_EQ(m_s1.test_bcast_num_connections(db, false), 0);
	ASSERT_EQ(m_s1.test_bcast_num_connections(db, true), 0);
	ASSERT_EQ(m_s2.test_bcast_num_connections(db, false), 0);
	ASSERT_EQ(m_s2.test_bcast_num_connections(db, true), 0);
}

TEST_F(stest, bcast_one_to_one_bidirectional_different_servers)
{
	std::vector<connection::cid_t> ids1 = {0, 500};
	std::vector<connection::cid_t> ids2 = {100, 600};

	set_ids(ids1, ids2);

	node c1, c2, c3;
	uint64_t db = 123;

	uint64_t c1_bias = 10000;
	uint64_t c2_bias = 30000;


	std::mutex lock;
	std::condition_variable cv1, cv2;

	std::atomic_ulong c1_counter(0), c1_completed(0);
	c1.connect(m_addr1, [&] (connection::pointer cn, message &msg) {
					VLOG(2) << "connection: " << cn->connection_string() <<
						", client1 received message: " << msg.to_string();
					ASSERT_EQ(msg.hdr.id, c2_bias + c1_counter);
					c1_counter++;
			});
	c1.bcast_join(db);

	std::atomic_ulong c2_counter(0), c2_completed(0);
	c2.connect(m_addr2, [&] (connection::pointer cn, message &msg) {
					VLOG(2) << "connection: " << cn->connection_string() <<
						", client2 received message: " << msg.to_string();
					ASSERT_EQ(msg.hdr.id, c1_bias + c2_counter);
					c2_counter++;
			});
	c2.bcast_join(db);

	connect_servers();

	uint64_t n = 10000;
	for (uint64_t i = 0; i < n; ++i) {
		message msg;
		msg.hdr.id = c1_bias + i;
		msg.hdr.db = db;
		msg.hdr.cmd = SCATTER_CMD_CLIENT + 1;
		msg.hdr.flags = SCATTER_FLAGS_NEED_ACK;
		msg.encode_header();

		c1.send(msg, [&] (connection::pointer, message &reply) {
					ASSERT_EQ(reply.hdr.id, c1_bias + c1_completed.load());
					c1_completed++;
					cv1.notify_one();
				});
		message m2;
		m2.hdr.id = c2_bias + i;
		m2.hdr.db = db;
		m2.hdr.cmd = SCATTER_CMD_CLIENT + 1;
		m2.hdr.flags = SCATTER_FLAGS_NEED_ACK;
		m2.encode_header();
		c2.send(m2, [&] (connection::pointer, message &reply) {
					ASSERT_EQ(reply.hdr.id, c2_bias + c2_completed.load());
					c2_completed++;
					cv2.notify_one();
				});
	}

	VLOG(2) << "going to wait" <<
		": c1_completed: " << c1_completed <<
		", c1_counter: " << c1_counter <<
		": c2_completed: " << c2_completed <<
		", c2_counter: " << c2_counter <<
		", n: " << n;

	std::unique_lock<std::mutex> l1(lock);
	cv1.wait_for(l1, std::chrono::seconds(1000), [&] {return c1_completed == n;});
	l1.unlock();

	ASSERT_EQ(c1_completed, n);
	ASSERT_EQ(c1_completed, c2_counter);

	std::unique_lock<std::mutex> l2(lock);
	cv2.wait_for(l2, std::chrono::seconds(1000), [&] {return c2_completed == n;});
	l2.unlock();

	ASSERT_EQ(c2_completed, n);
	ASSERT_EQ(c2_completed, c1_counter);

	return;
}

TEST_F(stest, connection_expiration)
{
	node c1, c2;
	uint64_t db = 123;

	uint64_t c2_bias = 30000;


	std::mutex lock;
	std::condition_variable cv1, cv2;

	std::atomic_ulong c1_counter(0), c1_missed(0);
	c1.connect(m_addr1, [&] (connection::pointer cn, message &msg) {
					c1_counter++;

					if (msg.hdr.id % 2 == 0) {
						VLOG(2) << "connection: " << cn->connection_string() <<
							", client1 received message: " << msg.to_string() <<
							", do not send reply";

						msg.hdr.flags = 0;
						c1_missed++;
					}
			});
	c1.bcast_join(db);

	std::atomic_ulong c2_counter(0), c2_completed(0), c2_failed(0);
	c2.connect(m_addr1, [&] (connection::pointer cn, message &msg) {
					VLOG(2) << "connection: " << cn->connection_string() <<
						", client2 received message: " << msg.to_string();

					c2_counter++;
			});
	c2.bcast_join(db);

	uint64_t n = 10;
	for (uint64_t i = 0; i < n; ++i) {
		message m2;
		m2.hdr.id = c2_bias + i;
		m2.hdr.db = db;
		m2.hdr.cmd = SCATTER_CMD_CLIENT + 1;
		m2.hdr.flags = SCATTER_FLAGS_NEED_ACK;
		m2.encode_header();
		c2.send(m2, [&] (connection::pointer, message &reply) {
					c2_completed++;

					if (reply.hdr.status != 0)
						c2_failed++;

					cv2.notify_one();
				});
	}

	std::unique_lock<std::mutex> l2(lock);
	cv2.wait_for(l2, std::chrono::seconds(20), [&] {return c2_completed == n;});
	l2.unlock();

	ASSERT_EQ(c2_completed, n);
	ASSERT_EQ(c2_failed, c1_missed);
	ASSERT_EQ(c1_counter, n);

	// needed to check the logs whether all intermediate transactions have expired
	sleep(1);

	return;
}


int main(int argc, char **argv)
{
	google::InitGoogleLogging(argv[0]);
	::testing::InitGoogleTest(&argc, argv);
	return RUN_ALL_TESTS();
}
