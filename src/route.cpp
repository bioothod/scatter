#include "scatter/route.hpp"

namespace ioremap { namespace scatter {

void route::add(connection::pointer cn)
{
	const std::vector<connection::cid_t> &cids = cn->ids();
	std::lock_guard<std::mutex> guard(m_lock);
	for (auto &id: cids) {
		m_connections[id] = cn;
	}
}

void route::remove(connection::pointer cn)
{
	const std::vector<connection::cid_t> &cids = cn->ids();
	std::lock_guard<std::mutex> guard(m_lock);
	for (auto &id: cids) {
		m_connections.erase(id);
	}
}

connection::pointer route::find(uint64_t id)
{
	std::lock_guard<std::mutex> guard(m_lock);
	auto it = m_connections.lower_bound(id);
	if (it == m_connections.end())
		return connection::pointer();

	return it->second;
}

}} // namespace ioremap::scatter
