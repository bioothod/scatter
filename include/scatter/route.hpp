#pragma once

#include "scatter/message.hpp"
#include "scatter/connection.hpp"

namespace ioremap { namespace scatter {

class route {
public:
	void add(connection::pointer);
	void remove(connection::pointer);

	connection::pointer find(uint64_t);

private:
	std::mutex m_lock;
	std::map<connection::cid_t, connection::pointer> m_connections;
};

}} // namespace ioremap::scatter
