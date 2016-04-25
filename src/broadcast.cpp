#include "scatter/broadcast.hpp"

namespace ioremap { namespace scatter {

broadcast::broadcast(uint64_t id) : m_id(id)
{
}

broadcast::broadcast(broadcast &&other)
	: m_id(other.m_id)
	, m_clients(std::move(other.m_clients))
	, m_servers(std::move(other.m_servers))
{
}

void broadcast::join(connection::pointer client, bool server_connection)
{
	std::lock_guard<std::mutex> m_guard(m_lock);

	if (server_connection)
		m_servers.insert(client);
	else
		m_clients.insert(client);

	VLOG(1) << "connection: " << client->connection_string() <<
			", broadcast: " << m_id <<
			", server_connection: " << server_connection <<
			", command: join" <<
			", clients: " << m_clients.size() <<
			", servers: " << m_servers.size();
}

void broadcast::join(connection::pointer client)
{
	join(client, false);
}

void broadcast::leave(connection::pointer client, connection::process_fn_t complete)
{
	std::lock_guard<std::mutex> m_guard(m_lock);
	size_t removed_clients = m_clients.erase(client);
	size_t removed_servers = m_servers.erase(client);

	if (!removed_clients && !removed_servers)
		return;

	VLOG(1) << "connection: " << client->connection_string() <<
			", broadcast: " << m_id <<
			", command: leave" <<
			", clients: " << m_clients.size() <<
			", servers: " << m_servers.size();

	// leave all groups which are connected over server-server connections
	if (m_clients.empty() && (m_servers.size() != 0)) {
		struct tmp {
			std::atomic_int refcnt;
			connection::process_fn_t complete;
		};

		auto shared(std::make_shared<tmp>());
		shared->refcnt = m_servers.size();
		shared->complete = complete;

		for (auto srv: m_servers) {
			srv->send(0, m_id, SCATTER_FLAGS_NEED_ACK, SCATTER_CMD_BCAST_LEAVE, NULL, 0,
				[shared, client] (connection::pointer srv, message &reply) {
					LOG(INFO) << "client connection: " << client->connection_string() <<
						", srv connection: " << srv->connection_string() <<
						", refcnt: " << shared->refcnt <<
						", reply: " << reply.to_string();
					if (--shared->refcnt == 0) {
						shared->complete(client, reply);
					}
				});
		}
	} else {
		message msg;
		msg.hdr.flags = SCATTER_FLAGS_REPLY;
		msg.hdr.db = m_id;
		complete(client, msg);
	}
}

void broadcast::send(message &msg, connection::process_fn_t complete)
{
	send(std::shared_ptr<connection>(), msg, complete);
}

// message must be already encoded
void broadcast::send(connection::pointer self, message &msg, connection::process_fn_t complete)
{
	std::unique_lock<std::mutex> guard(m_lock);
	std::vector<connection::pointer> copy;
	copy.reserve(m_clients.size() + m_servers.size());

	copy.insert(copy.end(), m_clients.begin(), m_clients.end());
	copy.insert(copy.end(), m_servers.begin(), m_servers.end());
	guard.unlock();

	auto cmp = std::make_shared<connection::completion_t>(self, msg, complete);
	cmp->completed = copy.size();
	cmp->err = -ENOENT;

	for (auto &c : copy) {
		VLOG(1) << "connection: " << self->connection_string() <<
			", broadcast connection: " << c->connection_string() <<
			": message: " << msg.to_string() <<
			", completed: " << cmp->completed << "/" << copy.size() <<
			", use_count: " << cmp.use_count();

			if (c == self) {
				send_completed(cmp, self, msg);
				continue;
			}

			// we basically copy message here, since its header will be modified (transaction number set),
			/// and this can happen in parallel for different connections
			c->send(msg.hdr.id, msg.hdr.db, msg.hdr.flags, msg.hdr.cmd, msg.data(), msg.hdr.size,
					std::bind(&broadcast::send_completed, this, cmp, std::placeholders::_1, std::placeholders::_2));
	}
}

void broadcast::send_completed(connection::shared_completion_t cmp, connection::pointer fwd, message &reply)
{
	if (fwd != cmp->self) {
		if (!reply.hdr.status)
			cmp->err = 0;
	}

	if (cmp->completed <= 0) {
		LOG(ERROR) << __PRETTY_FUNCTION__ << "bug"
			": completed: " << cmp->completed <<
			", self: " << cmp->self->connection_string() <<
			", fwd: " << fwd->connection_string() <<
			", reply: " << reply.to_string();

		*(char *)0 = 0;
		throw_error(-EINVAL, "found bug: completed: %d, self: %s, fwd: %s, reply: %s",
				cmp->completed.load(),
				cmp->self->connection_string().c_str(),
				fwd->connection_string().c_str(),
				reply.to_string().c_str());
	}

	if (--cmp->completed == 0) {
		VLOG(2) << "connection: " << cmp->self->connection_string() <<
			", broadcast connection: " << fwd->connection_string() <<
			", reply: " << reply.to_string();

		message tmp;
		tmp.hdr = cmp->copy.hdr;
		tmp.hdr.flags = SCATTER_FLAGS_REPLY;
		tmp.hdr.size = 0;
		tmp.hdr.status = cmp->err;
		if (reply.hdr.status && !cmp->err)
			tmp.hdr.status = reply.hdr.status;

		cmp->complete(cmp->self, tmp);
	}
}

size_t broadcast::num_clients() const
{
	return m_clients.size();
}
size_t broadcast::num_servers() const
{
	return m_servers.size();
}

}} // namespace ioremap::scatter
