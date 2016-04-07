#include "scatter/server.hpp"

namespace ioremap { namespace scatter {

server::server(io_service_pool& io_pool, const connection::proto::endpoint &ep, accept_fn_t accept)
: m_io_pool(io_pool),
  m_acceptor(io_pool.get_service(), ep),
  m_socket(io_pool.get_service()),
  m_accept(accept)
{
	schedule_accept();
}

void server::schedule_accept()
{
	m_acceptor.async_accept(m_socket,
			[this] (const boost::system::error_code &ec) {
				m_accept(ec, std::move(m_socket));
			});
}

connection::proto::socket& server::socket()
{
	return m_socket;
}

}} // namespace ioremap::scatter
