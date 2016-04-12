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

broadcast::~broadcast()
{
	// leave all groups which are connected over server-server connections
	message msg;
	msg.hdr.cmd = SCATTER_CMD_BCAST_LEAVE;
	msg.hdr.db = m_id;
	msg.encode_header();

	for (auto srv: m_servers) {
		srv->send(msg, [&] (connection::pointer, message &) {});
	}
}

void broadcast::join(connection::pointer client, bool server_connection)
{
	std::lock_guard<std::mutex> m_guard(m_lock);

	if (server_connection)
		m_servers.insert(client);
	else
		m_clients.insert(client);

	VLOG(2) << "broadcast: " << m_id <<
			", connection: " << client->connection_string() <<
			", command: join";
}

void broadcast::join(connection::pointer client)
{
	join(client, false);
}

void broadcast::leave(connection::pointer client)
{
	std::lock_guard<std::mutex> m_guard(m_lock);
	m_clients.erase(client);
	m_servers.erase(client);

	VLOG(2) << "broadcast: " << m_id <<
			", connection: " << client->connection_string() <<
			", command: leave";
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

	int err = -ENOENT;
	if (copy.size() == 1)
		err = 0;

	struct tmp {
		std::atomic_int			completed;
		int				err;
		connection::process_fn_t	complete;

		tmp(long c, int e, connection::process_fn_t f) : completed(c), err(e), complete(f) {}
	};

	auto var(std::make_shared<tmp>(copy.size(), err, complete));

	for (auto &c : copy) {
		VLOG(1) << "connection: " << self->connection_string() <<
			", broadcast connection: " << c->connection_string() <<
			": message: " << msg.to_string() <<
			", completed: " << var->completed << "/" << copy.size();

			if (self && (c->socket().local_endpoint() == self->socket().local_endpoint()) &&
					(c->socket().remote_endpoint() == self->socket().remote_endpoint())) {
				int cmp = --var->completed;

				VLOG(2) << "connection: " << self->connection_string() <<
					": message: " << msg.to_string() <<
					", completed: " << cmp;

				if (cmp == 0) {
					msg.hdr.status = var->err;
					var->complete(self, msg);
					return;
				}
				continue;
			}

			c->send(msg, [this, self, var] (connection::pointer fwd, message &reply) {
						if (reply.hdr.status) {
							leave(fwd);
						} else {
							// clear error if there is at least one successful sending and ack
							var->err = 0;
						}

						int cmp = --var->completed;

						VLOG(2) << "connection: " << self->connection_string() <<
							", broadcast connection: " << fwd->connection_string() <<
							", reply: " << reply.to_string() << ", completed: " << cmp;

						if (cmp == 0) {
							reply.hdr.status = var->err;
							var->complete(self, reply);
							return;
						}
					});
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
