#include "scatter/route.hpp"

#include <glog/logging.h>

namespace ioremap { namespace scatter {

void route::add(connection::pointer cn)
{
	std::vector<connection::cid_t> cids = cn->ids();
	std::ostringstream ss;

	std::lock_guard<std::mutex> guard(m_lock);
	for (auto &id: cids) {
		m_connections[id] = cn;
		ss << id;
		if (id != cids.back())
			ss << ",";
	}

	LOG(INFO) << "connection: " << cn->connection_string() << ", added routes: " << ss.str();
}

void route::remove(connection::pointer cn)
{
	std::vector<connection::cid_t> cids = cn->ids();
	std::ostringstream ss;

	std::lock_guard<std::mutex> guard(m_lock);
	for (auto &id: cids) {
		m_connections.erase(id);
		ss << id;
		if (id != cids.back())
			ss << ",";
	}
	LOG(INFO) << "connection: " << cn->connection_string() << ", removed routes: " << ss.str();
}

connection::pointer route::find(uint64_t id)
{
	std::lock_guard<std::mutex> guard(m_lock);
	auto it = m_connections.lower_bound(id);
	if (it == m_connections.end()) {
		if (m_connections.size()) {
			return m_connections[0];
		}

		return connection::pointer();
	}

	return it->second;
}

}} // namespace ioremap::scatter
