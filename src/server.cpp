#include "scatter/server.hpp"

#include <msgpack.hpp>

namespace ioremap { namespace scatter {

server::server(const std::string &addr, int io_pool_size)
	: m_io_pool(io_pool_size)
	, m_resolver(m_io_pool)
	, m_acceptor(m_io_pool.get_service(), m_resolver.resolve(addr).get()->endpoint(), true)
	, m_socket(m_io_pool.get_service()),
	m_announce_address(m_acceptor.local_endpoint())
{
	generate_ids();

	auto self = connection::create_empty(m_io_pool);
	self->set_ids(m_cids);
	self->set_announce_address(m_announce_address);
	m_route.add(self);

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

void server::drop_from_broadcast_group_nolock(uint64_t group, connection::pointer client, connection::process_fn_t complete)
{
	LOG(INFO) << "connection: " << client->connection_string() << ", leaving broadcast group: " << group;

	auto it = m_bcast.find(group);
	if (it == m_bcast.end()) {
		message msg;
		msg.hdr.db = group;
		complete(connection::pointer(), msg);
		return;
	}

	it->second.leave(client, complete);
	if (it->second.num_clients() == 0) {
		m_bcast.erase(it);
	}
}

void server::drop_from_broadcast_groups(connection::pointer client)
{
	std::unique_lock<std::mutex> guard(m_lock);

	// a little bit awkward way to leave many groups at once
	std::vector<uint64_t> groups;
	for (auto &p: m_bcast) {
		groups.push_back(p.first);
	}

	for (auto group: groups) {
		drop_from_broadcast_group_nolock(group, client, [] (connection::pointer, message &) {});
	}
}

void server::drop(connection::pointer cn, const boost::system::error_code &ec)
{
	(void) ec;

	m_route.remove(cn);

	drop_from_broadcast_groups(cn);
}

bool server::connection_to_self(connection::pointer cn)
{
	return cn->ids() == m_cids;
}

void server::join(connection::pointer srv)
{
	join_control ctl;
	ctl.cids = m_cids;
	ctl.addr = m_announce_address;

	std::stringstream buffer;
	msgpack::pack(buffer, ctl);
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

	if (!connection_to_self(cn))
		cn->send_blocked_command(m_cids[0], db, cmd, data, size);
}

bool server::announce_broadcast_group_nolock(uint64_t group, connection::pointer client, connection::process_fn_t complete)
{
	auto it = m_bcast.find(group);
	if (it == m_bcast.end()) {
		message msg;
		msg.hdr.db = group;
		complete(connection::pointer(), msg);
		return false;
	}

	auto cn = m_route.find(group);
	if (!cn || connection_to_self(cn) || (client && cn == client)) {
		message msg;
		msg.hdr.db = group;
		complete(cn, msg);
		return false;
	}

	LOG(INFO) << "connection: " << cn->connection_string() << ", announcing broadcast group: " << group;

	// when remote node gets new message into broadcast group @group from some other client, it will
	// be sent to this server and will be broadcasted further to clients
	cn->send(m_cids[0], group, SCATTER_FLAGS_NEED_ACK, SCATTER_CMD_BCAST_JOIN, NULL, 0, complete);

	// when this node gets new message into broadcast group @group,
	// it should also broadcast it to remote server
	it->second.join(cn, true);
	return true;
}

void server::announce_broadcast_groups(connection::pointer client, message &msg)
{
	struct completion {
		uint64_t trans;
		uint64_t id;
		uint64_t db;
		connection::pointer cn;
		std::atomic_int refcnt;
	};

	std::unique_lock<std::mutex> guard(m_lock);

	auto cm = std::make_shared<completion>();
	cm->cn = client;
	cm->refcnt = m_bcast.size() + 1;
	cm->id = msg.hdr.id;
	cm->db = msg.hdr.db;
	cm->trans = msg.hdr.trans;

	VLOG(2) << "connection: " << client->connection_string() << ", message: " << msg.to_string() <<
		", refcnt: " << cm->refcnt;

	auto completion = [cm] (connection::pointer, message &) {
		VLOG(2) << "completion: refcnt: " << cm->refcnt;
		if (--cm->refcnt == 0) {
			message reply;
			reply.hdr.db = cm->db;
			reply.hdr.id = cm->id;
			reply.hdr.trans = cm->trans;
			cm->cn->send_reply(reply);
		}
	};

	for (auto &bg: m_bcast) {
		uint64_t group = bg.first;

		// we should send all broadcast groups into @client
		announce_broadcast_group_nolock(group, connection::pointer(), completion);
	}

	completion(client, msg);
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
		announce_broadcast_group_nolock(msg.db(), client, [msg, client] (connection::pointer, message &reply) mutable {
							msg.hdr.status = reply.hdr.status;
							client->send_reply(msg);
						});

		msg.hdr.flags &= ~SCATTER_FLAGS_NEED_ACK;
		msg.hdr.status = 0;
		break;
	}
	case SCATTER_CMD_BCAST_LEAVE: {
		std::unique_lock<std::mutex> guard(m_lock);

		drop_from_broadcast_group_nolock(msg.db(), client, [msg, client] (connection::pointer, message &) {
							client->send_reply(msg);
						});

		msg.hdr.flags &= ~SCATTER_FLAGS_NEED_ACK;
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

			join_control ctl;
			up.get().convert(&ctl);

			client->set_ids(ctl.cids);
			client->set_announce_address(ctl.addr);
			m_route.add(client);

			announce_broadcast_groups(client, msg);
			// ack will be sent after all broadcast groups have been announced
			msg.hdr.flags &= ~SCATTER_FLAGS_NEED_ACK;
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
	case SCATTER_CMD_CONNECTIONS: {
		auto cns = m_route.connections();
		std::vector<address> addrs;
		for (auto &cn: cns) {
			if (!cn || connection_to_self(cn))
				continue;

			const auto &addr = cn->announce_address();
			addrs.push_back(addr);
		}

		std::stringstream buffer;
		msgpack::pack(buffer, addrs);
		std::string rdata(buffer.str());

		client->send(msg.hdr.id, msg.hdr.db, SCATTER_FLAGS_REPLY, SCATTER_CMD_CONNECTIONS, rdata.data(), rdata.size(),
				[] (connection::pointer, message &) {});

		// server has sent reply, no need to send another one
		msg.hdr.flags &= ~SCATTER_FLAGS_NEED_ACK;
		break;
	}
	default:
		break;
	}
}

void server::test_set_ids(const std::vector<connection::cid_t> &cids)
{
	auto self = connection::create_empty(m_io_pool);
	self->set_ids(m_cids);
	m_route.remove(self);

	m_cids = cids;
	self->set_ids(m_cids);
	m_route.add(self);
}
std::vector<connection::cid_t> server::test_ids() const
{
	return m_cids;
}
int server::test_bcast_num_connections(uint64_t db, bool server)
{
	int num = 0;
	std::unique_lock<std::mutex> guard(m_lock);
	auto it = m_bcast.find(db);
	if (it != m_bcast.end()) {
		if (server)
			num = it->second.num_servers();
		else
			num = it->second.num_clients();
	}
	return num;
}

}} // namespace ioremap::scatter
