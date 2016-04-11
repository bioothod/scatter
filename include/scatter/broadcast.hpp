#pragma once

#include "scatter/connection.hpp"

#include <set>

namespace ioremap { namespace scatter {

class broadcast {
public:
	broadcast(uint64_t id);
	broadcast(broadcast &&other);

	~broadcast();

	template <typename C>
	static void create_and_insert(C &collection, uint64_t id, connection::pointer client) {
		auto it = collection.find(id);
		if (it == collection.end()) {
			broadcast d(id);
			d.join(client);
			collection.insert(std::pair<uint64_t, broadcast>(id, std::move(d)));
		} else {
			it->second.join(client);
		}
	}

	void join(connection::pointer client);
	void join(connection::pointer client, bool server_connection);
	void leave(connection::pointer client);

	size_t num_clients() const;
	size_t num_servers() const;

	// message must be already encoded
	void send(message &msg, connection::process_fn_t complete);
	void send(connection::pointer self, message &msg, connection::process_fn_t complete);

private:
	uint64_t m_id;

	std::mutex m_lock;
	std::set<connection::pointer> m_clients;

	// these connections are server-server connections
	std::set<connection::pointer> m_servers;
};



}} // namespace ioremap::scatter
