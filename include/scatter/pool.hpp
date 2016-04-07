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

class io_service_pool {
public:
	io_service_pool(int num);
	~io_service_pool();

	boost::asio::io_service &get_service();

	void queue_task(std::function<void ()> fn);

private:
	std::vector<std::thread> m_pool;
	boost::asio::io_service m_io_service;
	std::unique_ptr<boost::asio::io_service::work> m_work;

	void process();
};

}} // namespace ioremap::scatter
