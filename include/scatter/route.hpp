#pragma once

#include "scatter/message.hpp"
#include "scatter/connection.hpp"

#include <set>

namespace ioremap { namespace scatter {

class route {
public:
	int add(connection::pointer);
	int remove(connection::pointer);

	std::set<connection::pointer> connections();

	connection::pointer find(uint64_t);

private:
	std::mutex m_lock;
	std::map<connection::cid_t, connection::pointer> m_connections;
};

}} // namespace ioremap::scatter
