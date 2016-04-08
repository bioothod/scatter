#include "scatter/pool.hpp"
#include <glog/logging.h>

namespace ioremap { namespace scatter {

io_service_pool::io_service_pool(int num) : m_work(new boost::asio::io_service::work(m_io_service)) {
	for (int i = 0; i < num; ++i) {
		m_pool.emplace_back(std::thread(std::bind(&io_service_pool::process, this)));
	}
}

io_service_pool::~io_service_pool()
{
	stop();

	for (auto &th: m_pool) {
		th.join();
	}
}

void io_service_pool::stop()
{
	m_work.reset();
	m_io_service.stop();
}

boost::asio::io_service &io_service_pool::get_service() {
	return m_io_service;
}

void io_service_pool::queue_task(std::function<void ()> fn) {
	m_io_service.post(fn);
}

void io_service_pool::process() {
	try {
		m_io_service.run();
	} catch (...) {
	}
}

}} // namespace ioremap::scatter
