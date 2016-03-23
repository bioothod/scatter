#pragma once

#if 0
#define BOOST_THREAD_PROVIDES_FUTURE
#define BOOST_THREAD_PROVIDES_FUTURE_CONTINUATION
#include <boost/thread/future.hpp>
#endif

#include <boost/bind.hpp> // must be first
#include <boost/asio.hpp>

#include <future>
#include <list>
#include <mutex>
#include <thread>
#include <vector>

namespace ioremap { namespace scatter {

using handler_fn_t = std::function<void ()>;

class io_service_pool {
public:
	io_service_pool(int num) : m_work(new boost::asio::io_service::work(m_io_service)) {
		for (int i = 0; i < num; ++i) {
			m_pool.emplace_back(std::thread(std::bind(&io_service_pool::process, this)));
		}
	}

	~io_service_pool() {
		m_work.reset();

		for (auto &th: m_pool) {
			th.join();
		}
	}

	boost::asio::io_service &get_service() {
		return m_io_service;
	}

	void queue_task(handler_fn_t fn) {
		m_io_service.post(fn);
	}

private:
	std::vector<std::thread> m_pool;
	boost::asio::io_service m_io_service;
	std::unique_ptr<boost::asio::io_service::work> m_work;

	void process() {
		try {
			m_io_service.run();
		} catch (...) {
		}
	}
};

}} // namespace ioremap::scatter
