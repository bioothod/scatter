#pragma once

#include "scatter/connection.hpp"
#include "scatter/server.hpp"

namespace ioremap { namespace scatter {

class node {
public:
	node();
	node(const std::string &addr_str);
	~node();

	connection::pointer connect(const std::string &addr, typename connection::process_fn_t process);

	void join(connection::pointer cn, uint64_t db_id);

	// message should be encoded
	void send(message &msg, connection::process_fn_t complete);

private:
	uint64_t m_id;

	std::unique_ptr<io_service_pool> m_io_pool;
	std::unique_ptr<resolver<>> m_resolver;

	std::mutex m_lock;
	std::unique_ptr<server> m_server;

	std::map<uint64_t, broadcast> m_bcast;

	void init(int io_pool_size);
	void drop(connection::pointer cn, const boost::system::error_code &ec);
};

}} // namespace ioremap::scatter
