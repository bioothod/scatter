#include "scatter/map.hpp"
#include "scatter/server.hpp"

#include <gtest/gtest.h>

using namespace ioremap::scatter;

class dmap_test : public ::testing::Test {
protected:
	dmap_test()
	: m_addr1("127.0.0.1:21001:2")
	, m_addr2("127.0.0.1:21002:2")
	, m_srv1(m_addr1, 4)
	, m_srv2(m_addr2, 4)
	{
		LOG(INFO) << "New dmap test starts";

		m_srv1.test_set_ids({0, 500});
		m_srv2.test_set_ids({100, 600});

		connection::pointer cn = m_srv2.connect(m_addr1);
		m_srv2.join(cn);
	}

	virtual ~dmap_test() {
	}

	std::string m_addr1, m_addr2;
	server m_srv1, m_srv2;
};

TEST_F(dmap_test, insert)
{
	int num = 1000;
	int db = 123;

	dmap c1(1, db);
	c1.connect(m_addr1);
	std::condition_variable cv1;
	std::atomic_int c1_completed(0);

	dmap c2(1, db);
	c2.connect(m_addr2);
	std::condition_variable cv2;
	std::atomic_int c2_completed(0);

	std::vector<dkey_package> keys1, keys2;

	for (int i = 0; i < num; ++i) {
		std::string key = "key." + std::to_string(i);
		dkey data;
		data.bucket = "data.bucket." + std::to_string(i % 10);
		data.key = "data.key." + std::to_string(i);

		keys1.push_back({key, data});
		c1.insert(key, data, [&] (connection::pointer, message &) {
					c1_completed++;
					cv1.notify_one();
				});

		data.bucket = "another.data.bucket." + std::to_string(i % 10);
		data.key = "another.data.key." + std::to_string(i);
		keys2.push_back({key, data});

		c2.insert(key, data, [&] (connection::pointer, message &) {
					c2_completed++;
					cv2.notify_one();
				});
	}

	std::mutex lock;
	std::unique_lock<std::mutex> l(lock);
	cv1.wait_for(l, std::chrono::seconds(10), [&] {return c1_completed == num;});
	l.unlock();

	ASSERT_EQ(c1_completed, num);
	ASSERT_EQ((int)keys1.size(), num);

	l.lock();
	cv2.wait_for(l, std::chrono::seconds(10), [&] {return c2_completed == num;});
	l.unlock();

	ASSERT_EQ(c2_completed, num);
	ASSERT_EQ((int)keys2.size(), num);

	for (int i = 0; i < num; ++i) {
		auto &kp = keys1[i];
		auto vec = c1.find(kp.key);
		ASSERT_EQ((int)vec.size(), 2);

		int found = 0;
		for (auto &v: vec) {
			if (v == keys1[i].data) {
				++found;
				continue;
			}

			if (v == keys2[i].data) {
				++found;
				continue;
			}
		}

		ASSERT_EQ(found, (int)vec.size());
	}
}

int main(int argc, char **argv)
{
	google::InitGoogleLogging(argv[0]);
	::testing::InitGoogleTest(&argc, argv);
	return RUN_ALL_TESTS();
}
