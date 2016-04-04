#include "scatter/node.hpp"

namespace ioremap { namespace scatter {

node::node()
{
	init(1);
}
node::node(const std::string &addr_str)
{
	init(5);

	m_server.reset(new server(*m_io_pool, m_resolver->resolve(addr_str).get()->endpoint(),
			[this] (const boost::system::error_code &ec, connection::proto::socket &&socket) {
				if (!ec) {
					connection::pointer client = connection::create(*m_io_pool,
							std::bind(&node::message_handler, this,
								std::placeholders::_1, std::placeholders::_2),
							std::bind(&node::drop, this,
								std::placeholders::_1, std::placeholders::_2),
							std::move(socket));

					LOG(INFO) << "server: accepted new client: " << client->connection_string();

					std::unique_lock<std::mutex> guard(m_lock);
					m_connected[client->socket().remote_endpoint()] = client;
					guard.unlock();

					client->start_reading();
				}

				// reschedule acceptor
				m_server->schedule_accept();
			}));
}
node::~node()
{
}

connection::pointer node::connect(const std::string &addr, typename connection::process_fn_t process)
{
	connection::pointer client = connection::create(*m_io_pool, process,
			std::bind(&node::drop, this, std::placeholders::_1, std::placeholders::_2));

	LOG(INFO) << "connecting to addr: " << addr << ", id: " << m_id;

	client->connect(m_resolver->resolve(addr).get());

	LOG(INFO) << "connected to addr: " << addr << ", id: " << m_id;
	return client;
}

void node::join(connection::pointer cn, uint64_t db)
{
	message msg(0);

	msg.hdr.id = m_id;
	msg.hdr.db = db;
	msg.hdr.cmd = SCATTER_CMD_JOIN;
	msg.hdr.flags = SCATTER_FLAGS_NEED_ACK;

	LOG(INFO) << "joining: " <<
		": id: " << m_id <<
		", db: " << db <<
		std::endl;

	msg.encode_header();

	std::promise<int> p;
	std::future<int> f = p.get_future();

	cn->send(msg,
		[&] (scatter::connection::pointer self, scatter::message &msg) {
			if (msg.hdr.status) {
				p.set_exception(std::make_exception_ptr(create_error(msg.hdr.status,
							"could not join database id: %ld, error: %d",
							db, msg.hdr.status)));
				return;
			}

			std::unique_lock<std::mutex> guard(m_lock);
			broadcast::create_and_insert(m_bcast, db, self);
			guard.unlock();

			LOG(INFO) << "joined: " <<
				": id: " << m_id <<
				", db: " << db <<
				std::endl;

			p.set_value(db);
		});

	f.get();
}

void node::drop(connection::pointer cn, const boost::system::error_code &ec)
{
	(void) ec;

	std::unique_lock<std::mutex> guard(m_lock);

	m_connected.erase(cn->socket().remote_endpoint());

	for (auto &p : m_bcast) {
		broadcast &bcast = p.second;

		bcast.leave(cn);
	}
}

// message should be encoded
void node::send(message &msg, connection::process_fn_t complete)
{
	long db = msg.db();

	std::unique_lock<std::mutex> guard(m_lock);
	auto it = m_bcast.find(db);
	if (it == m_bcast.end()) {
		throw_error(-ENOENT, "node didn't join to database %ld", db);
	}

	it->second.send(msg, complete);
}

void node::init(int io_pool_size)
{
	m_id = rand();

	m_io_pool.reset(new io_service_pool(io_pool_size));
	m_resolver.reset(new resolver<>(*m_io_pool));
}

void node::forward_message(connection::pointer client, message &msg)
{
	std::unique_lock<std::mutex> guard(m_lock);
	auto it = m_bcast.find(msg.db());
	if (it == m_bcast.end()) {
		guard.unlock();
		msg.hdr.status = -ENOENT;
		return;
	}

	auto sptr = msg.raw_buffer();
	LOG(INFO) << "forward: db: " << msg.db() <<
		", message: " << msg.to_string();
	it->second.send(client, msg, [this, sptr] (connection::pointer self, message &reply) {
				LOG(INFO) << "forward: connection: " << self->connection_string() <<
					", db: " << reply.db() <<
					", reply: " << reply.to_string();

				self->send_reply(reply);
			});
	guard.unlock();

	// clear need-ack bit to prevent server from sending ack back to client
	// since we have to wait for all connections in broadcast group to receive this
	// message and send reply, and only after that ack can be sent to the original client
	msg.hdr.flags &= ~SCATTER_FLAGS_NEED_ACK;
}

// message has been already decoded
// processing function should not send ack itself,
// if it does send ack, it has to clear SCATTER_FLAGS_NEED_ACK bit in msg.flags,
// otherwise connection's code will send another ack
void node::message_handler(connection::pointer client, message &msg)
{
	LOG(INFO) << "server received message: " << msg.to_string();

	if (msg.hdr.cmd >= SCATTER_CMD_CLIENT) {
		forward_message(client, msg);
		return;
	}

	switch (msg.hdr.cmd) {
	case SCATTER_CMD_JOIN: {
		std::unique_lock<std::mutex> guard(m_lock);

		auto it = m_connected.find(client->socket().remote_endpoint());
		if (it == m_connected.end()) {
			LOG(ERROR) <<
				"remote_endpoint: " << client->socket().remote_endpoint().address() <<
					":" << client->socket().remote_endpoint().port() <<
				", message: " << msg.to_string() <<
				": could not find remote endpoint in the list of connected sockets, probably double join";
			msg.hdr.status = -ENOENT;
			break;
		}

		broadcast::create_and_insert(m_bcast, msg.db(), client);
		msg.hdr.status = 0;
		break;
	}
	default:
		break;
	}
}

}} // namespace ioremap::scatter
