#include "scatter/server.hpp"

#include <msgpack.hpp>

namespace ioremap { namespace scatter {

server::server(const std::string &addr, int io_pool_size)
	: m_io_pool(io_pool_size)
	, m_resolver(m_io_pool)
	, m_acceptor(m_io_pool.get_service(), m_resolver.resolve(addr).get()->endpoint(), true)
	, m_socket(m_io_pool.get_service())
{
	generate_ids();
	schedule_accept();
}
server::~server()
{
	m_io_pool.stop();
}

void server::generate_ids()
{
	m_cids.resize(10);
	for (auto &id: m_cids) {
		id = rand();
	}
}

void server::schedule_accept()
{
	m_acceptor.async_accept(m_socket,
			[this] (const boost::system::error_code &ec) {
				if (!ec) {
					connection::pointer client = connection::create(m_io_pool,
							std::bind(&server::message_handler, this,
								std::placeholders::_1, std::placeholders::_2),
							std::bind(&server::drop, this,
								std::placeholders::_1, std::placeholders::_2),
							std::move(m_socket));

					LOG(INFO) << "server: accepted new client: " << client->connection_string();

					client->start_reading();
				} else {
					LOG(ERROR) << "server: error: " << ec.message();
				}

				// reschedule acceptor
				schedule_accept();
			});
}

connection::pointer server::connect(const std::string &addr)
{
	connection::pointer srv = connection::create(m_io_pool,
			std::bind(&server::message_handler, this, std::placeholders::_1, std::placeholders::_2),
			std::bind(&server::drop, this, std::placeholders::_1, std::placeholders::_2));

	srv->connect(m_resolver.resolve(addr).get());
	m_route.add(srv);

	return srv;
}

void server::drop_from_broadcast_group(connection::pointer cn)
{
	std::vector<uint64_t> remove;
	std::unique_lock<std::mutex> guard(m_lock);

	for (auto &group: m_bcast) {
		group.second.leave(cn);
		if (!group.second.size())
			remove.push_back(group.first);
	}

	for (auto id: remove) {
		m_bcast.erase(id);
	}
}

void server::drop(connection::pointer cn, const boost::system::error_code &ec)
{
	(void) ec;

	m_route.remove(cn);

	drop_from_broadcast_group(cn);
}

void server::join(connection::pointer srv)
{
	std::stringstream buffer;
	msgpack::pack(buffer, m_cids);
	std::string buf(buffer.str());

	srv->send_blocked_command(m_cids[0], 0, SCATTER_CMD_SERVER_JOIN, buf.data(), buf.size());
}

void server::send_blocked_command(uint64_t db, int cmd, const char *data, size_t size)
{
	auto cn = m_route.find(db);
	if (!cn) {
		LOG(ERROR) << "send_blocked_command: db: " << db <<
			", command: " << cmd <<
			", error: node is not connected to anything which can handle this database";

		throw_error(-ENOENT, "node is not connected to anything which can handle database %ld", db);
	}

	cn->send_blocked_command(m_cids[0], db, cmd, data, size);
}

void server::announce_broadcast_groups(connection::pointer connected)
{
	std::unique_lock<std::mutex> guard(m_lock);
	for (auto &bg: m_bcast) {
		uint64_t group = bg.first;

		auto cn = m_route.find(group);
		if (!cn)
			continue;

		LOG(INFO) << "connection: " << connected->connection_string() <<
			", announcing broadcast group: " << group <<
			", check connection: " << cn->connection_string();

		if (cn->socket().remote_endpoint() == connected->socket().remote_endpoint()) {
			LOG(INFO) << "connection: " << cn->connection_string() << ", announcing broadcast group: " << group;

			// when remote node gets new message into broadcast group @group from some other client, it will
			// be sent to this server and will be broadcasted further to clients
			connected->send(m_cids[0], group, 0, SCATTER_CMD_BCAST_JOIN, NULL, 0, [&] (connection::pointer, message &) {});

			// when this node gets new message into broadcast group @group,
			// it should also broadcast it to remote server
			bg.second.join(cn);
		}
	}
}

void server::broadcast_client_message(connection::pointer client, message &msg)
{
	std::unique_lock<std::mutex> guard(m_lock);
	auto it = m_bcast.find(msg.db());
	if (it == m_bcast.end()) {
		guard.unlock();
		msg.hdr.status = -ENOENT;
		return;
	}

	auto sptr = msg.raw_buffer();
	VLOG(1) << "broadcast_client_message: connection: " << client->connection_string() <<
		", message: " << msg.to_string();

	it->second.send(client, msg, [this, sptr] (connection::pointer self, message &reply) {
				VLOG(1) << "broadcast_client_message: connection: " << self->connection_string() <<
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
void server::message_handler(connection::pointer client, message &msg)
{
	if (msg.hdr.cmd >= SCATTER_CMD_CLIENT) {
		VLOG(1) << "connection: " << client->connection_string() << ", server received message: " << msg.to_string();
		broadcast_client_message(client, msg);
		return;
	}

	LOG(INFO) << "connection: " << client->connection_string() << ", server received message: " << msg.to_string();
	switch (msg.hdr.cmd) {
	case SCATTER_CMD_BCAST_JOIN: {
		std::unique_lock<std::mutex> guard(m_lock);

		broadcast::create_and_insert(m_bcast, msg.db(), client);
		msg.hdr.status = 0;
		break;
	}
	case SCATTER_CMD_BCAST_LEAVE: {
		drop_from_broadcast_group(client);
		msg.hdr.status = 0;
		break;
	}
	case SCATTER_CMD_REMOTE_IDS: {
		std::stringstream buffer;
		msgpack::pack(buffer, m_cids);

		std::string rdata(buffer.str());

		client->send(msg.hdr.id, msg.hdr.db, SCATTER_FLAGS_REPLY, SCATTER_CMD_REMOTE_IDS, rdata.data(), rdata.size(),
				[] (connection::pointer, message &) {});

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

			announce_broadcast_groups(client);
		} catch (const std::exception &e) {
			LOG(ERROR) << "connection: " << client->connection_string() <<
				", message: " << msg.to_string() <<
				", error: could not unpack ids: " << e.what();

			msg.hdr.status = -EINVAL;
			return;
		}
		msg.hdr.status = 0;
		break;
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

void server::test_set_ids(const std::vector<connection::cid_t> &cids)
{
	m_cids = cids;
}
std::vector<connection::cid_t> server::test_ids() const
{
	return m_cids;
}

}} // namespace ioremap::scatter
