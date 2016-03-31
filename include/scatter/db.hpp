#pragma once

#include "scatter/connection.hpp"

#include <set>

namespace ioremap { namespace scatter {

class db {
public:
	db(uint64_t id) : m_id(id) {}
	db(db &&other)
	: m_id(other.m_id)
	, m_clients(other.m_clients) {
	}

	template <typename C>
	static void create_and_insert(C &collection, uint64_t id, connection::pointer client) {
		auto db_it = collection.find(id);
		if (db_it == collection.end()) {
			db d(id);
			d.join(client);
			collection.insert(std::pair<uint64_t, db>(id, std::move(d)));
		} else {
			db_it->second.join(client);
		}
	}

	void join(connection::pointer client) {
		std::lock_guard<std::mutex> m_guard(m_lock);
		m_clients.insert(client);

		LOG(INFO) << "db: " << m_id <<
				", connection: " << client->connection_string() <<
				", command: join";
	}

	void leave(connection::pointer client) {
		std::lock_guard<std::mutex> m_guard(m_lock);
		m_clients.erase(client);

		LOG(INFO) << "db: " << m_id <<
				", connection: " << client->connection_string() <<
				", command: leave";
	}

	void send(message &msg, connection::process_fn_t complete) {
		send(std::shared_ptr<connection>(), msg, complete);
	}

	// message must be already encoded
	void send(connection::pointer self, message &msg, connection::process_fn_t complete) {
		std::unique_lock<std::mutex> guard(m_lock);
		std::vector<connection::pointer> copy(m_clients.begin(), m_clients.end());
		guard.unlock();

		std::atomic_int completed(copy.size());
		for (auto &c : copy) {
			LOG(INFO) << "db: " << m_id <<
				": broadcasting message: " << msg.to_string() <<
				", connection: " << c->connection_string() <<
				", completed: " << completed << "/" << copy.size();

			if (self && (c->socket().local_endpoint() == self->socket().local_endpoint()) &&
					(c->socket().remote_endpoint() == self->socket().remote_endpoint())) {
				if (--completed == 0) {
					complete(self, msg);
				}
				continue;
			}

			auto buf = msg.raw_buffer();
			c->send(msg, [this, buf, self, complete, &completed] (connection::pointer fwd, message &reply) {
						if (reply.hdr.status) {
							leave(fwd);
						}

						if (--completed == 0) {
							complete(self, reply);
						}
					});
		}
	}

private:
	uint64_t m_id;

	std::mutex m_lock;
	std::set<connection::pointer> m_clients;
	std::deque<message> m_log;
};



}} // namespace ioremap::scatter
