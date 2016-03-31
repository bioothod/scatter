#include "scatter/server.hpp"

namespace ioremap { namespace scatter {

server::server(io_service_pool& io_pool, const boost::asio::ip::tcp::endpoint &ep)
: m_io_pool(io_pool),
  m_acceptor(io_pool.get_service(), ep),
  m_socket(io_pool.get_service()) {
	start_accept();
}

void server::forward_message(connection::pointer client, message &msg)
{
	std::unique_lock<std::mutex> guard(m_lock);
	auto it = m_dbs.find(msg.db());
	if (it == m_dbs.end()) {
		guard.unlock();
		msg.hdr.status = -ENOENT;
		return;
	}

	auto sptr = msg.raw_buffer();
	int error = -ENOENT;
	LOG(INFO) << "forward: db: " << msg.db() <<
		", message: " << msg.to_string();
	it->second.send(client, msg, [this, sptr, &error] (connection::pointer self, message &reply) {
				LOG(INFO) << "forward: connection: " << self->connection_string() <<
					", db: " << reply.db() <<
					", reply: " << reply.to_string();
				if (!reply.hdr.status) {
					error = 0;
				}
			});
	guard.unlock();

	msg.hdr.status = error;
}

// message has been already decoded
// processing function should not send ack itself,
// if it does send ack, it has to clear SCATTER_FLAGS_NEED_ACK bit in msg.flags,
// otherwise connection's code will send another ack
void server::message_handler(connection::pointer client, message &msg)
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

		db::create_and_insert(m_dbs, msg.hdr.db, client);
		msg.hdr.status = 0;
		break;
	}
	default:
		break;
	}
}

void server::start_accept()
{
	m_acceptor.async_accept(m_socket,
			[this] (boost::system::error_code ec) {
				if (!ec) {
					connection::pointer client = connection::create(m_io_pool,
							std::bind(&server::message_handler, this,
								std::placeholders::_1, std::placeholders::_2),
							std::bind(&server::drop, this,
								std::placeholders::_1, std::placeholders::_2),
							std::move(m_socket));

					std::unique_lock<std::mutex> guard(m_lock);
					m_connected[client->socket().remote_endpoint()] = client;
					guard.unlock();

					client->start_reading();
				}

				// reschedule acceptor
				start_accept();
			});
}

void server::drop(connection::pointer cn, const boost::system::error_code &ec)
{
	std::unique_lock<std::mutex> guard(m_lock);
	m_connected.erase(cn->socket().remote_endpoint());

	for (auto &p : m_dbs) {
		db &db = p.second;

		db.leave(cn);
	}
}

}} // namespace ioremap::scatter
