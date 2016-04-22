#pragma once

#include "scatter/address.hpp"
#include "scatter/message.hpp"
#include "scatter/pool.hpp"

#include <ribosome/expiration.hpp>

#include <boost/bind.hpp> // must be first among boost headers
#include <boost/asio.hpp>
#include <boost/asio/use_future.hpp>

#include <glog/logging.h>

#include <deque>
#include <unordered_map>

namespace ioremap { namespace scatter {

class connection : public std::enable_shared_from_this<connection>
{
public:
	typedef std::shared_ptr<connection> pointer;
	typedef boost::asio::ip::tcp proto;
	typedef typename proto::resolver::iterator resolver_iterator;

	typedef std::function<void (pointer client, message &)> process_fn_t;
	typedef std::function<void (pointer client, const boost::system::error_code &)> error_fn_t;

	typedef uint64_t cid_t;

	static pointer create(io_service_pool& io_pool, process_fn_t process, error_fn_t error, typename proto::socket &&socket) {
		return pointer(new connection(io_pool, process, error, std::move(socket)));
	}
	static pointer create(io_service_pool& io_pool, process_fn_t process, error_fn_t error) {
		return pointer(new connection(io_pool, process, error));
	}
	static pointer create_empty(io_service_pool &io_pool) {
		return pointer(new connection(io_pool));
	}

	proto::socket& socket();
	std::string connection_string() const;
	std::string remote_string() const;
	std::string local_string() const;

	void set_announce_address(const address &addr);
	const address &announce_address() const;

	void close();
	void start_reading();

	void connect(const resolver_iterator it);
	void request_remote_nodes(process_fn_t);

	// message should not be encoded
	// it will be modified, @hdr.trans will be set
	void send(message &msg, process_fn_t complete);

	// data will be copied into newly created message
	void send(uint64_t id, uint64_t db, uint64_t flags, int cmd, const char *data, size_t size, process_fn_t complete);

	void send_reply(const message &msg);

	// data will be copied into newly created message
	void send_blocked_command(uint64_t id, uint64_t db, int cmd, const char *data, size_t size);

	const std::vector<cid_t> ids() const;
	void set_ids(const std::vector<cid_t> &cids);

	~connection();
private:
	io_service_pool &m_pool;
	boost::asio::io_service::strand m_strand;
	process_fn_t m_process;
	error_fn_t m_error;

	proto::socket m_socket;
	std::string m_local_string;
	std::string m_remote_string;
	header m_tmp_hdr;

	std::atomic_long m_transactions;

	address m_announce_address;

	ribosome::expiration m_expire;

	struct completion_t {
		message			copy;
		process_fn_t		complete;
		ribosome::expiration::token_t	expiration_token = 0;

		completion_t(const message &msg, process_fn_t c) : copy(msg), complete(c) {}
	};
	typedef std::shared_ptr<completion_t> shared_completion_t;

	std::mutex m_lock;
	std::unordered_map<uint64_t, shared_completion_t> m_sent;
	std::deque<shared_completion_t> m_outgoing;

	// we have connected to remote node, these are ids for that remote node
	std::vector<cid_t> m_cids;

	connection(io_service_pool &io, process_fn_t process, error_fn_t errror, typename proto::socket &&socket);
	connection(io_service_pool &io, process_fn_t process, error_fn_t error);
	connection(io_service_pool &io);

	void strand_write_callback(shared_completion_t);
	void write_next_buf(message::raw_buffer_t buf);
	void write_completed(const boost::system::error_code &error, size_t bytes_transferred);

	void read_header();
	void read_data(std::shared_ptr<message> msg);
	void process_message(std::shared_ptr<message> msg);
	void process_reply(message &msg);

	// called from @connect() method to obtain ids of the remote node
	void request_remote_ids(std::promise<int> &p);
	void parse_remote_ids(std::promise<int> &p, message &msg);

	void set_connection_strings();
};

}} // namespace ioremap::scatter
