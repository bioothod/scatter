#include "scatter/node.hpp"
#include "scatter/serialize.hpp"

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

connection::pointer node::connect(const std::string &addr_str, typename connection::process_fn_t process)
{
	connection::pointer cn = connection::create(m_io_pool, process,
			std::bind(&node::drop, this, std::placeholders::_1, std::placeholders::_2));

	auto it = m_resolver.resolve(addr_str).get();

	cn->connect(it);
	m_route.add(cn);

	std::promise<int> p;
	std::future<int> f = p.get_future();

	std::vector<address> addrs;
	cn->request_remote_nodes([&] (connection::pointer, message &msg) {
				if (msg.hdr.status) {
					LOG(ERROR) << "connection: " << cn->connection_string() <<
						", reply: " << msg.to_string() <<
						", error: could not request remote connections";

					p.set_exception(std::make_exception_ptr(
							create_error(msg.hdr.status, "connect: could not request remote connections")));
					return;
				}

				try {
					msgpack::unpacked up;
					msgpack::unpack(&up, msg.data(), msg.hdr.size);

					up.get().convert(&addrs);
					p.set_value(0);
				} catch (const std::exception &e) {
					LOG(ERROR) << "connection: " << cn->connection_string() <<
						", message: " << msg.to_string() <<
						", error: could not unpack array of endpoints: " << e.what();

					p.set_exception(std::current_exception());
					return;
				}
			});

	f.get();

	for (auto &addr: addrs) {
		LOG(INFO) << "received address: " << addr.to_string();
		if (addr.endpoint() == cn->socket().remote_endpoint())
			continue;

		auto eps_it = connection::resolver_iterator::create(addr.endpoint(), addr.host(), std::to_string(addr.port()));
		connection::pointer c = connection::create(m_io_pool, process,
				std::bind(&node::drop, this, std::placeholders::_1, std::placeholders::_2));

		c->connect(eps_it);
		m_route.add(c);
	}
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
void node::bcast_leave(uint64_t db)
{
	send_blocked_command(db, SCATTER_CMD_BCAST_LEAVE, NULL, 0);
}

void node::drop(connection::pointer cn, const boost::system::error_code &ec)
{
	(void) ec;

	m_route.remove(cn);
}

// message should not be encoded
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
