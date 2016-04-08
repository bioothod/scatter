#pragma once

#include "scatter/connection.hpp"
#include "scatter/route.hpp"
#include "scatter/server.hpp"

namespace ioremap { namespace scatter {

class node {
public:
	node(int io_pool_size);
	node();
	~node();

	// this node connects to remote address @addr and adds given connection into local route table
	connection::pointer connect(const std::string &addr, typename connection::process_fn_t process);
	void drop(connection::pointer cn, const boost::system::error_code &ec);

	void bcast_join(uint64_t db);

	connection::pointer get_connection(uint64_t db);

	// message should be encoded
	void send(message &msg, connection::process_fn_t complete);

private:
	uint64_t m_id;
	route m_route;

	// io_service must be destroyed first, since it can
	// invoke @drop() method which touches other class members.
	io_service_pool m_io_pool;
	resolver<> m_resolver;

	void send_blocked_command(connection::pointer cn, uint64_t db, int cmd, const char *data, size_t size);
	void send_blocked_command(uint64_t db, int cmd, const char *data, size_t size);

};

}} // namespace ioremap::scatter
