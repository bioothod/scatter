#include "scatter/route.hpp"

#include <glog/logging.h>

namespace ioremap { namespace scatter {

int route::add(connection::pointer cn)
{
	std::vector<connection::cid_t> cids = cn->ids();
	std::ostringstream ss;
	int err = -EEXIST;

	std::lock_guard<std::mutex> guard(m_lock);
	for (auto &id: cids) {
		auto it = m_connections.find(id);
		if (it != m_connections.end()) {
			continue;
		}

		m_connections.insert(std::pair<connection::cid_t, connection::pointer>(id, cn));
		err = 0;

		ss << id;
		if (id != cids.back())
			ss << ",";
	}

	LOG(INFO) << "connection: " << cn->connection_string() << ", added routes: " << ss.str() << ", error: " << err;
	return err;
}

int route::remove(connection::pointer cn)
{
	std::vector<connection::cid_t> cids = cn->ids();
	std::ostringstream ss;
	int err = -ENOENT;

	std::lock_guard<std::mutex> guard(m_lock);
	for (auto &id: cids) {
		auto it = m_connections.find(id);
		if (it == m_connections.end()) {
			continue;
		}

		if (it->second != cn)
			continue;

		m_connections.erase(it);
		err = 0;

		ss << id;
		if (id != cids.back())
			ss << ",";
	}
	LOG(INFO) << "connection: " << cn->connection_string() << ", removed routes: " << ss.str() << ", error: " << err;
	return err;
}

connection::pointer route::find(uint64_t id)
{
	std::lock_guard<std::mutex> guard(m_lock);
	if (m_connections.empty())
		return connection::pointer();

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

std::set<connection::pointer> route::connections()
{
	std::set<connection::pointer> cns;
	std::lock_guard<std::mutex> guard(m_lock);
	for (const auto &p : m_connections) {
		cns.insert(p.second);
	}

	return cns;
}

}} // namespace ioremap::scatter
