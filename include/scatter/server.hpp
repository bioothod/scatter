#pragma once

#include "scatter/broadcast.hpp"
#include "scatter/connection.hpp"
#include "scatter/pool.hpp"
#include "scatter/route.hpp"

namespace ioremap { namespace scatter {

class server {
public:
	server(const std::string &addr, int io_pool_size);
	~server();

	connection::pointer connect(const std::string &addr);
	void join(connection::pointer srv);


	// this should only be used by testing system to simulate node initialization
	// these test methods are not thread-safe and should be called when it is guaranteed
	// that route info hasn't been sent to remote nodes yet
	void test_set_ids(const std::vector<connection::cid_t> &cids);
	std::vector<connection::cid_t> test_ids() const;

private:
	io_service_pool m_io_pool;
	resolver<> m_resolver;
	connection::proto::acceptor m_acceptor;
	connection::proto::socket m_socket;

	route m_route;

	std::mutex m_lock;
	std::map<uint64_t, broadcast> m_bcast;
	std::vector<connection::cid_t> m_cids;

	void broadcast_client_message(connection::pointer client, message &msg);

	void generate_ids();
	void schedule_accept();

	void drop(connection::pointer cn, const boost::system::error_code &ec);
	void send_blocked_command(uint64_t db, int cmd, const char *data, size_t size);

	// message has been already decoded
	// processing function should not send ack itself,
	// if it does send ack, it has to clear SCATTER_FLAGS_NEED_ACK bit in msg.flags,
	// otherwise connection's code will send another ack
	void message_handler(connection::pointer client, message &msg);
};

}} // namespace ioremap::scatter
