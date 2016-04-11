#include "scatter/node.hpp"

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
	std::vector<connection::cid_t> ids2 = {200, 600};

	set_ids(ids1, ids2);
	connect_servers();

	node c;

	c.connect(m_addr1, [&] (connection::pointer, message &) {});
	auto cn = c.get_connection(123);
	ASSERT_NE(cn.use_count(), 0);
	ASSERT_EQ(cn->remote_string(), m_addr1);

	cn = c.get_connection(1000000);
	ASSERT_NE(cn.use_count(), 0);
	ASSERT_EQ(cn->remote_string(), m_addr1);

	c.connect(m_addr2, [&] (connection::pointer, message &) {});
	cn = c.get_connection(123);
	ASSERT_NE(cn.use_count(), 0);
	ASSERT_EQ(cn->remote_string(), m_addr2);

	cn = c.get_connection(1000000);
	ASSERT_NE(cn.use_count(), 0);
	ASSERT_EQ(cn->remote_string(), m_addr1);

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

	std::atomic_int c1_counter(0), c1_completed(0);
	c1.connect(m_addr1, [&] (connection::pointer, message &) {
					c1_counter++;
			});
	c1.bcast_join(db);

	std::atomic_int c2_counter(0);
	c2.connect(m_addr1, [&] (connection::pointer, message &) {
					c2_counter++;
			});
	c2.bcast_join(db);

	int c1_bias = 10000;

	int n = 10000;
	for (int i = 0; i < n; ++i) {
		message msg;
		msg.hdr.id = c1_bias + i;
		msg.hdr.db = db;
		msg.hdr.cmd = SCATTER_CMD_CLIENT + 1;
		msg.hdr.flags = SCATTER_FLAGS_NEED_ACK;
		msg.encode_header();

		c1.send(msg, [&] (connection::pointer, message &reply) {
					ASSERT_EQ(reply.id(), c1_bias + c1_completed.load());
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

	std::atomic_int c1_counter(0), c1_completed(0);
	c1.connect(m_addr1, [&] (connection::pointer, message &) {
					c1_counter++;
			});
	c1.bcast_join(db);

	std::atomic_int c2_counter(0), c2_completed(0);
	c2.connect(m_addr1, [&] (connection::pointer, message &) {
					c2_counter++;
			});
	c2.bcast_join(db);

	int c1_bias = 10000;
	int c2_bias = 30000;

	int n = 10000;
	for (int i = 0; i < n; ++i) {
		message msg;
		msg.hdr.id = c1_bias + i;
		msg.hdr.db = db;
		msg.hdr.cmd = SCATTER_CMD_CLIENT + 1;
		msg.hdr.flags = SCATTER_FLAGS_NEED_ACK;
		msg.encode_header();

		c1.send(msg, [&] (connection::pointer, message &reply) {
					ASSERT_EQ(reply.id(), c1_bias + c1_completed.load());
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
					ASSERT_EQ(reply.id(), c2_bias + c2_completed.load());
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

	std::atomic_int c1_counter(0), c1_completed(0);
	c1.connect(m_addr1, [&] (connection::pointer, message &) {
					c1_counter++;
			});
	c1.bcast_join(db);

	std::atomic_int c2_counter(0), c2_completed(0);
	c2.connect(m_addr1, [&] (connection::pointer, message &) {
					c2_counter++;
			});
	c2.bcast_join(db);

	std::atomic_int c3_counter(0), c3_completed(0);
	c3.connect(m_addr1, [&] (connection::pointer, message &) {
					c3_counter++;
			});
	c3.bcast_join(db);

	int c1_bias = 10000;

	int n = 10000;
	for (int i = 0; i < n; ++i) {
		message msg;
		msg.hdr.id = c1_bias + i;
		msg.hdr.db = db;
		msg.hdr.cmd = SCATTER_CMD_CLIENT + 1;
		msg.hdr.flags = SCATTER_FLAGS_NEED_ACK;
		msg.encode_header();

		c1.send(msg, [&] (connection::pointer, message &reply) {
					ASSERT_EQ(reply.id(), c1_bias + c1_completed.load());
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

	std::atomic_int c1_counter(0), c1_completed(0);
	c1.connect(m_addr1, [&] (connection::pointer, message &) {
					c1_counter++;
			});
	c1.bcast_join(db);

	std::atomic_int c2_counter(0), c2_completed(0);
	c2.connect(m_addr1, [&] (connection::pointer, message &) {
					c2_counter++;
			});
	c2.bcast_join(db);

	std::atomic_int c3_counter(0), c3_completed(0);
	c3.connect(m_addr1, [&] (connection::pointer, message &) {
					c3_counter++;
			});
	c3.bcast_join(db);

	int c1_bias = 10000;
	int c2_bias = 30000;

	int n = 10000;
	for (int i = 0; i < n; ++i) {
		message msg;
		msg.hdr.id = c1_bias + i;
		msg.hdr.db = db;
		msg.hdr.cmd = SCATTER_CMD_CLIENT + 1;
		msg.hdr.flags = SCATTER_FLAGS_NEED_ACK;
		msg.encode_header();

		c1.send(msg, [&] (connection::pointer, message &reply) {
					ASSERT_EQ(reply.id(), c1_bias + c1_completed.load());
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
					ASSERT_EQ(reply.id(), c2_bias + c2_completed.load());
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


int main(int argc, char **argv)
{
	google::InitGoogleLogging(argv[0]);
	::testing::InitGoogleTest(&argc, argv);
	return RUN_ALL_TESTS();
}
