#pragma once

#include "scatter/connection.hpp"
#include "scatter/server.hpp"

namespace ioremap { namespace scatter {

class node {
public:
	node();
	node(const std::string &addr_str);
	~node();

	connection::pointer connect(const std::string &addr, typename connection::handler_fn_t fn);

	void join(connection::pointer cn, uint64_t db_id);

	// message should be encoded
	void send(message &msg, connection::handler_fn_t complete);

private:
	uint64_t m_id;

	std::unique_ptr<io_service_pool> m_io_pool;
	std::unique_ptr<resolver<>> m_resolver;

	std::mutex m_lock;
	std::unique_ptr<server> m_server;

	std::map<uint64_t, db> m_dbs;

	void init(int io_pool_size);
};

}} // namespace ioremap::scatter
