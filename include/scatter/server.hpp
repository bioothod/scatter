#pragma once

#include "scatter/connection.hpp"
#include "scatter/db.hpp"
#include "scatter/pool.hpp"

namespace ioremap { namespace scatter {

class server {
public:
	server(io_service_pool& io_pool, const boost::asio::ip::tcp::endpoint &ep);

private:
	typedef typename boost::asio::ip::tcp proto;

	io_service_pool &m_io_pool;
	proto::acceptor m_acceptor;
	proto::socket m_socket;

	std::mutex m_lock;
	std::map<uint64_t, db> m_dbs;
	std::map<typename proto::endpoint, connection::pointer> m_connected;

	void forward_message(connection::pointer client, message &msg);

	// message has been already decoded
	// processing function should not send ack itself,
	// if it does send ack, it has to clear SCATTER_FLAGS_NEED_ACK bit in msg.flags,
	// otherwise connection's code will send another ack
	void message_handler(connection::pointer client, message &msg);

	void start_accept();
};

}} // namespace ioremap::scatter
