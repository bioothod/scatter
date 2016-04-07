#pragma once

#include "scatter/broadcast.hpp"
#include "scatter/connection.hpp"
#include "scatter/pool.hpp"

namespace ioremap { namespace scatter {

class server {
public:
	typedef std::function<void (const boost::system::error_code &, connection::proto::socket &&)> accept_fn_t;

	server(io_service_pool& io_pool, const boost::asio::ip::tcp::endpoint &ep, accept_fn_t accept);

	connection::proto::socket &socket();

	void schedule_accept();
private:

	io_service_pool &m_io_pool;
	connection::proto::acceptor m_acceptor;
	connection::proto::socket m_socket;

	accept_fn_t m_accept;
};

}} // namespace ioremap::scatter
