#include "scatter/node.hpp"

#include <msgpack.hpp>

namespace ioremap { namespace scatter {

node::node(int io_pool_size)
	: m_io_pool(io_pool_size)
	, m_resolver(m_io_pool)
{
}
node::node() : node(1)
{
}
node::~node()
{
	m_io_pool.stop();
}

connection::pointer node::connect(const std::string &addr, typename connection::process_fn_t process)
{
	connection::pointer cn = connection::create(m_io_pool, process,
			std::bind(&node::drop, this, std::placeholders::_1, std::placeholders::_2));

	cn->connect(m_resolver.resolve(addr).get());
	m_route.add(cn);

	return cn;
}

void node::send_blocked_command(uint64_t db, int cmd, const char *data, size_t size)
{
	auto cn = m_route.find(db);
	if (!cn) {
		LOG(ERROR) << "send_blocked_command: db: " << db <<
			", command: " << cmd <<
			", error: node is not connected to anything which can handle this database";

		throw_error(-ENOENT, "node is not connected to anything which can handle database %ld", db);
	}

	cn->send_blocked_command(m_id, db, cmd, data, size);
}

void node::bcast_join(uint64_t db)
{
	send_blocked_command(db, SCATTER_CMD_BCAST_JOIN, NULL, 0);
}

void node::drop(connection::pointer cn, const boost::system::error_code &ec)
{
	(void) ec;

	m_route.remove(cn);
}

// message should be encoded
void node::send(message &msg, connection::process_fn_t complete)
{
	long db = msg.db();

	auto cn = m_route.find(db);
	if (!cn) {
		LOG(ERROR) << "send: message: " << msg.to_string() <<
			", error: node is not connected to anything which can handle database " << db;

		msg.hdr.status = -ENOENT;
		complete(cn, msg);
		return;
	}

	cn->send(msg, complete);
}

connection::pointer node::get_connection(uint64_t db)
{
	return m_route.find(db);
}

}} // namespace ioremap::scatter
