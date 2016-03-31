#pragma once

#include "scatter/message.hpp"
#include "scatter/pool.hpp"
#include "scatter/resolver.hpp"

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
	typedef std::function<void (const boost::system::error_code &, size_t)> handler_t;

	typedef std::function<void (pointer client, message &)> handler_fn_t;

	static pointer create(io_service_pool& io_pool, handler_fn_t fn, typename proto::socket &&socket) {
		return pointer(new connection(io_pool, fn, std::move(socket)));
	}
	static pointer create(io_service_pool& io_pool, handler_fn_t fn) {
		return pointer(new connection(io_pool, fn));
	}

	proto::socket& socket();
	std::string connection_string() const;

	void close();
	void start_reading();

	void connect(const resolver_iterator it);

	// message has to be already encoded
	void send(const message &msg, handler_fn_t fn);
	void send_reply(const message &msg);

private:
	io_service_pool &m_pool;
	boost::asio::io_service::strand m_strand;
	handler_fn_t m_fn;
	proto::socket m_socket;
	std::string m_local_string;
	std::string m_remote_string;
	message m_message;

	typedef struct {
		uint64_t		id;
		uint64_t		flags;
		message::raw_buffer_t	buf;
		handler_fn_t		complete;
	} completion_t;

	std::mutex m_lock;
	std::unordered_map<uint64_t, completion_t> m_sent;
	std::deque<completion_t> m_outgoing;

	connection(io_service_pool &io, handler_fn_t fn, typename proto::socket &&socket);
	connection(io_service_pool &io, handler_fn_t fn);

	void strand_write_callback(uint64_t id, uint64_t flags, message::raw_buffer_t buf, handler_fn_t fn);
	void write_next_buf(message::raw_buffer_t buf);
	void write_completed(const boost::system::error_code &error, size_t bytes_transferred);

	void read_header();
	void read_data();
	void process_message();
};

}} // namespace ioremap::scatter
