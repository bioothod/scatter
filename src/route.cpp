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

	// @it points to value >= than @id

	if (it == m_connections.end()) {
		// none is >= than @id, we select the last one

		if (m_connections.size()) {
			return m_connections.rbegin()->second;
		}

		return connection::pointer();
	}

	if (it->first == id)
		return it->second;

	// @it points to value which is strongly greater than @id
	//
	// we want @it to point to value which is first to be less than @id
	// can not just decrement iterator, since it can point to the smallest registered ID, i.e. be the first
	// in this case we wrap iterator and point to the last element
	if (it == m_connections.begin()) {
		if (m_connections.size()) {
			return m_connections.rbegin()->second;
		}

		return connection::pointer();
	}

	--it;

	return it->second;
}

}} // namespace ioremap::scatter
