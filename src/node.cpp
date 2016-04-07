#include "scatter/node.hpp"

#include <msgpack.hpp>

namespace ioremap { namespace scatter {

node::node()
{
	init(1);
}
node::node(const std::string &addr_str)
{
	init(5);

	generate_ids();
	//m_route.add(m_cids, std::make_shared<connection>());

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
				} else {
					LOG(ERROR) << "server: error: " << ec.message();
				}

				// reschedule acceptor
				if (m_server)
					m_server->schedule_accept();
			}));
}
node::~node()
{
	LOG(INFO) << "node " << m_id << " is going down";
}

void node::generate_ids()
{
	m_cids.resize(10);
	for (auto &id: m_cids) {
		id = rand();
	}
}

connection::pointer node::connect(const std::string &addr, typename connection::process_fn_t process)
{
	connection::pointer client = connection::create(*m_io_pool, process,
			std::bind(&node::drop, this, std::placeholders::_1, std::placeholders::_2));

	client->connect(m_resolver->resolve(addr).get());
	m_route.add(client);

	return client;
}

void node::send_blocked_command(connection::pointer cn, uint64_t db, int cmd, const char *data, size_t size)
{
	message msg(size);

	if (data && size)
		msg.append(data, size);

	msg.hdr.size = size;
	msg.hdr.id = m_id;
	msg.hdr.db = db;
	msg.hdr.cmd = cmd;
	msg.hdr.flags = SCATTER_FLAGS_NEED_ACK;
	msg.encode_header();

	std::promise<int> p;
	std::future<int> f = p.get_future();

	cn->send(msg,
		[&] (scatter::connection::pointer self, scatter::message &msg) {
			if (msg.hdr.status) {
				LOG(ERROR) << "connection: " << cn->connection_string() <<
					", message: " << msg.to_string() <<
					", error: could not send blocked command: " << cmd <<
					", db: " << db <<
					", error: " << msg.hdr.status;

				p.set_exception(std::make_exception_ptr(create_error(msg.hdr.status,
							"could not send blocked cmd: %d, database id: %ld, error: %d",
							cmd, db, msg.hdr.status)));
				return;
			}

			p.set_value(db);
		});

	f.get();
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

	send_blocked_command(cn, db, cmd, data, size);
}

void node::bcast_join(uint64_t db)
{
	send_blocked_command(db, SCATTER_CMD_BCAST_JOIN, NULL, 0);
}

void node::server_join(connection::pointer srv)
{
	std::stringstream buffer;
	msgpack::pack(buffer, m_cids);

	std::string buf(buffer.str());
	send_blocked_command(srv, 0, SCATTER_CMD_SERVER_JOIN, buf.data(), buf.size());
}

void node::drop(connection::pointer cn, const boost::system::error_code &ec)
{
	(void) ec;

	std::unique_lock<std::mutex> guard(m_lock);

	m_connected.erase(cn->socket().remote_endpoint());
	m_route.remove(cn);

	for (auto &p : m_bcast) {
		broadcast &bcast = p.second;

		bcast.leave(cn);
	}
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

void node::init(int io_pool_size)
{
	m_id = rand();

	m_io_pool.reset(new io_service_pool(io_pool_size));
	m_resolver.reset(new resolver<>(*m_io_pool));
	LOG(INFO) << "node " << m_id << " has been created";
}

void node::broadcast_client_message(connection::pointer client, message &msg)
{
	std::unique_lock<std::mutex> guard(m_lock);
	auto it = m_bcast.find(msg.db());
	if (it == m_bcast.end()) {
		guard.unlock();
		msg.hdr.status = -ENOENT;
		return;
	}

	auto sptr = msg.raw_buffer();
	LOG(INFO) << "broadcast_client_message: connection: " << client->connection_string() <<
		", message: " << msg.to_string();

	it->second.send(client, msg, [this, sptr] (connection::pointer self, message &reply) {
				LOG(INFO) << "broadcast_client_message: connection: " << self->connection_string() <<
					", completed with reply: " << reply.to_string();

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
		broadcast_client_message(client, msg);
		return;
	}

	switch (msg.hdr.cmd) {
	case SCATTER_CMD_BCAST_JOIN: {
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
	case SCATTER_CMD_BCAST_LEAVE: {
		std::unique_lock<std::mutex> guard(m_lock);

		for (auto &group: m_bcast) {
			group.second.leave(client);
		}

		msg.hdr.status = 0;
		break;
	}
	case SCATTER_CMD_REMOTE_IDS: {
		std::stringstream buffer;
		msgpack::pack(buffer, m_cids);

		std::string rdata(buffer.str());

		message reply(rdata.size());
		reply.append(rdata.data(), rdata.size());

		reply.hdr.size = rdata.size();
		reply.hdr.cmd = SCATTER_CMD_REMOTE_IDS;
		reply.hdr.flags = SCATTER_FLAGS_REPLY;
		reply.hdr.id = msg.hdr.id;
		reply.hdr.db = msg.hdr.db;
		reply.encode_header();

		client->send(reply, [] (connection::pointer, message &) {});

		// server has sent reply, no need to send another one
		msg.hdr.flags &= ~SCATTER_FLAGS_NEED_ACK;
		break;
	}
	case SCATTER_CMD_SERVER_JOIN: {
		try {
			msgpack::unpacked up;
			msgpack::unpack(&up, msg.data(), msg.hdr.size);

			std::vector<connection::cid_t> cids;
			up.get().convert(&cids);

			client->set_ids(cids);
			m_route.add(client);
		} catch (const std::exception &e) {
			LOG(ERROR) << "connection: " << client->connection_string() <<
				", message: " << msg.to_string() <<
				", error: could not unpack ids: " << e.what();

			msg.hdr.status = -EINVAL;
			return;
		}
		msg.hdr.status = 0;
	}
	case SCATTER_CMD_SERVER_LEAVE: {
		m_route.remove(client);
		msg.hdr.status = 0;
		break;
	}
	default:
		break;
	}
}

connection::pointer node::get_connection(uint64_t db)
{
	return m_route.find(db);
}

void node::test_set_ids(const std::vector<connection::cid_t> &cids)
{
	m_cids = cids;
}
std::vector<connection::cid_t> node::test_ids() const
{
	return m_cids;
}

}} // namespace ioremap::scatter
