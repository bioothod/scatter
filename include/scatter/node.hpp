#pragma once

#include "scatter/connection.hpp"
#include "scatter/route.hpp"
#include "scatter/server.hpp"

namespace ioremap { namespace scatter {

class node {
public:
	node();
	node(const std::string &addr_str);
	~node();

	// this node connects to remote address @addr and adds given connection into local route table
	connection::pointer connect(const std::string &addr, typename connection::process_fn_t process);

	void server_join(connection::pointer srv);
	void bcast_join(uint64_t db);

	connection::pointer get_connection(uint64_t db);

	// message should be encoded
	void send(message &msg, connection::process_fn_t complete);

	// this should only be used by testing system to simulate node initialization
	// these test methods are not thread-safe and should be called when it is guaranteed
	// that route info hasn't been sent to remote nodes yet
	void test_set_ids(const std::vector<connection::cid_t> &cids);
	std::vector<connection::cid_t> test_ids() const;

private:
	uint64_t m_id;

	std::unique_ptr<io_service_pool> m_io_pool;
	std::unique_ptr<resolver<>> m_resolver;

	std::mutex m_lock;
	std::unique_ptr<server> m_server;

	std::map<uint64_t, broadcast> m_bcast;

	std::vector<connection::cid_t> m_cids;
	route m_route;

	void init(int io_pool_size);
	void generate_ids();

	void drop(connection::pointer cn, const boost::system::error_code &ec);

	// these are part of the server node
	std::map<connection::proto::endpoint, connection::pointer> m_connected;
	void broadcast_client_message(connection::pointer client, message &msg);

	// message has been already decoded
	// processing function should not send ack itself,
	// if it does send ack, it has to clear SCATTER_FLAGS_NEED_ACK bit in msg.flags,
	// otherwise connection's code will send another ack
	void message_handler(connection::pointer client, message &msg);

	void send_blocked_command(connection::pointer cn, uint64_t db, int cmd, const char *data, size_t size);
	void send_blocked_command(uint64_t db, int cmd, const char *data, size_t size);
};

}} // namespace ioremap::scatter
