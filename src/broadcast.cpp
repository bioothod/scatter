#include "scatter/broadcast.hpp"

namespace ioremap { namespace scatter {

broadcast::broadcast(uint64_t id) : m_id(id)
{
}

broadcast::broadcast(broadcast &&other)
	: m_id(other.m_id)
	, m_clients(other.m_clients)
{
}

void broadcast::join(connection::pointer client)
{
	std::lock_guard<std::mutex> m_guard(m_lock);
	m_clients.insert(client);

	LOG(INFO) << "broadcast: " << m_id <<
			", connection: " << client->connection_string() <<
			", command: join";
}

void broadcast::leave(connection::pointer client)
{
	std::lock_guard<std::mutex> m_guard(m_lock);
	m_clients.erase(client);

	LOG(INFO) << "broadcast: " << m_id <<
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
	std::vector<connection::pointer> copy(m_clients.begin(), m_clients.end());
	guard.unlock();

	int err = -ENOENT;
	if (copy.size() == 1)
		err = 0;

	std::atomic_int completed(copy.size());
	for (auto &c : copy) {
		LOG(INFO) << "broadcast: " << m_id <<
			": broadcasting message: " << msg.to_string() <<
			", connection: " << c->connection_string() <<
			", completed: " << completed << "/" << copy.size();

			if (self && (c->socket().local_endpoint() == self->socket().local_endpoint()) &&
					(c->socket().remote_endpoint() == self->socket().remote_endpoint())) {
				if (--completed == 0) {
					msg.hdr.status = 0;
					complete(self, msg);
					return;
				}
				continue;
			}

			c->send(msg, [this, self, &err, &msg, complete, &completed] (connection::pointer fwd, message &reply) {
						if (reply.hdr.status) {
							leave(fwd);
						} else {
							// clear error if there is at least one successful sending and ack
							err = 0;
						}

						if (--completed == 0) {
							reply.hdr.status = err;
							complete(self, reply);
							return;
						}
					});
	}
}

}} // namespace ioremap::scatter