#include "scatter/node.hpp"

#include <gtest/gtest.h>

using namespace ioremap::scatter;

class stest : public ::testing::Test {
protected:
	stest()
	: m_addr1("127.0.0.1:21001:2")
	, m_addr2("127.0.0.1:21002:2")
	, m_s1(m_addr1)
	, m_s2(m_addr2)
	{
	}

	virtual ~stest() {
	}

	void set_ids(std::vector<connection::cid_t> &ids1, std::vector<connection::cid_t> &ids2) {
		m_s1.test_set_ids(ids1);
		m_s2.test_set_ids(ids2);
	}

	void connect_servers() {
		connection::pointer cn = m_s2.connect(m_addr1,
				std::bind(&stest::s2_client_process, this, std::placeholders::_1, std::placeholders::_2));
		m_s2.server_join(cn);
	}

	void s2_client_process(connection::pointer cn, message &msg) {
	}

	std::string m_addr1, m_addr2;
	node m_s1, m_s2;
};

TEST_F(stest, route)
{
	std::vector<connection::cid_t> ids1 = {0, 500};
	std::vector<connection::cid_t> ids2 = {100, 600};

	set_ids(ids1, ids2);
	connect_servers();

	node c;

	c.connect(m_addr1, [&] (connection::pointer, message &) {});
	auto cn = c.get_connection(100);
	ASSERT_NE(cn.use_count(), 0);
	ASSERT_EQ(cn->remote_string(), m_addr1);

	c.connect(m_addr2, [&] (connection::pointer, message &) {});
	cn = c.get_connection(100);
	ASSERT_EQ(cn->remote_string(), m_addr2);

	printf("completed\n");
	return;
}

int main(int argc, char **argv)
{
	::testing::InitGoogleTest(&argc, argv);
	return RUN_ALL_TESTS();
}
