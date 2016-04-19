#pragma once

#include "scatter/connection.hpp"
#include "scatter/resolver.hpp"
#include "scatter/route.hpp"

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
	void bcast_leave(uint64_t db);

	connection::pointer get_connection(uint64_t db);

	// message should be encoded
	void send(message &msg, connection::process_fn_t complete);

	uint64_t id() const;

private:
	io_service_pool m_io_pool;
	resolver<connection::proto> m_resolver;

	uint64_t m_id;
	route m_route;

	void send_blocked_command(connection::pointer cn, uint64_t db, int cmd, const char *data, size_t size);
	void send_blocked_command(uint64_t db, int cmd, const char *data, size_t size);

};

}} // namespace ioremap::scatter
