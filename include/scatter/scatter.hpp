#pragma once

#include "scatter/endian.hpp"
#include "scatter/exception.hpp"
#include "scatter/pool.hpp"
#include "scatter/resolver.hpp"

#include <boost/bind.hpp> // must be first
#include <boost/asio.hpp>
#include <boost/asio/use_future.hpp>

#include <glog/logging.h>

#include <set>

namespace ioremap { namespace scatter {

enum {
	SCATTER_CMD_SERVER	= 0,
	SCATTER_CMD_JOIN,
	SCATTER_CMD_CLIENT	= 1024,
	__SCATTER_CMD_MAX
};

// this message is a reply
#define SCATTER_FLAGS_REPLY	(1<<0)

struct header {
	uint64_t		id;
	uint64_t		db;
	int			cmd;
	int			status;
	uint64_t		flags;
	uint64_t		size;

	header() {
		memset((char *)&this->id, 0, sizeof(header));
	}

	void convert() {
		id = scatter_bswap64(id);
		db = scatter_bswap64(db);
		cmd = scatter_bswap32(cmd);
		status = scatter_bswap32(status);
		flags = scatter_bswap64(flags);
		size = scatter_bswap64(size);
	}
};

class message {
public:
	header			hdr;

	enum {
		header_size = sizeof(header)
	};

	message() : message(0) {
		LOG(INFO) << "this: " << this << ", created empty message buffer";
	}

	message(size_t size) {
		m_buffer = std::make_shared<std::vector<char>>(size + sizeof(header));
		LOG(INFO) << "this: " << this << ", created message buffer with size: " << size;
		advance(sizeof(header));
	}

	bool decode_header() {
		hdr = *reinterpret_cast<header *>(buffer());
		convert_header();
		return true;
	}
	bool encode_header() {
		convert_header();
		memcpy(buffer(), &hdr, sizeof(header));
		return true;
	}

	void resize(size_t size) {
		m_buffer->resize(size + sizeof(header));
	}

	const char *buffer() const {
		return m_buffer->data();
	}
	char *buffer() {
		return const_cast<char *>(m_buffer->data());
	}

	const char *data() const {
		return m_buffer->data() + m_data_offset;
	}
	char *data() {
		return const_cast<char *>(m_buffer->data() + m_data_offset);
	}


	size_t total_size() const {
		return m_buffer->size();
	}

	size_t headroom() const {
		return m_data_offset;
	}

	void advance(size_t size) {
		if (m_data_offset + size > m_buffer->size()) {
			throw_error(-EINVAL, "advance: data_offset: %zd, buffer_size: %zd, size: %zd, error: too large size to advance",
					m_data_offset, m_buffer->size(), size);
		}

		m_data_offset += size;
	}

private:
	// this buffer has enough size to host header and data
	std::shared_ptr<std::vector<char>> m_buffer;

	bool m_cpu = true;
	size_t m_data_offset = 0;

	void convert_header() {
		if (!m_cpu)
			return;

		hdr.convert();
		m_cpu = false;
	}
};

class connection : public std::enable_shared_from_this<connection>
{
public:
	typedef std::shared_ptr<connection> pointer;
	typedef boost::asio::ip::tcp proto;
	typedef typename proto::resolver::iterator resolver_iterator;

	static pointer create(io_service_pool& io_pool, typename proto::socket &&socket) {
		return pointer(new connection(io_pool, std::move(socket)));
	}
	static pointer create(io_service_pool& io_pool) {
		return pointer(new connection(io_pool));
	}

	proto::socket& socket() {
		return m_socket;
	}

	void close() {
		m_pool.queue_task([this]() { m_socket.close(); });
	}

	void start_reading() {
		read_header();
	}

	std::future<resolver_iterator> connect(const resolver_iterator it) {
		return boost::asio::async_connect(m_socket, it, boost::asio::use_future);
	}

	std::future<std::size_t> send(const message &msg) {
		return boost::asio::async_write(m_socket,
				boost::asio::buffer(msg.buffer(), msg.total_size()),
				boost::asio::use_future);
	}

private:
	io_service_pool &m_pool;
	proto::socket m_socket;
	message m_message;

	connection(io_service_pool &io, typename proto::socket &&socket)
		: m_pool(io)
		, m_socket(std::move(socket)) {}
	connection(io_service_pool &io)
		: m_pool(io)
		, m_socket(m_pool.get_service()) {}

	void handle_write(const boost::system::error_code &error, size_t bytes_transferred) {
		(void)error;
		(void)bytes_transferred;
	}

	void read_header() {
		auto self(shared_from_this());

		boost::asio::async_read(m_socket,
			boost::asio::buffer(m_message.buffer(), message::header_size),
				[this, self] (boost::system::error_code ec, std::size_t /*size*/) {
					if (ec || !m_message.decode_header()) {
						// reset connection, drop it from database
						return;
					}

					read_data();
				});
	}

	void read_data() {
		auto self(shared_from_this());

		m_message.resize(m_message.hdr.size);
		boost::asio::async_read(m_socket,
			boost::asio::buffer(m_message.data(), m_message.hdr.size),
				[this, self] (boost::system::error_code ec, std::size_t /*size*/) {
					if (ec) {
						// reset connection, drop it from database
						return;
					}
					
					// we have whole message, reschedule read
					// schedule message processing into separate thread pool
					// or process it locally (here)
					//
					// if seprate pool is used, @m_message must be a pointer
					// which will have to be moved to that pool
					// and new pointer must be created to read next message into

					LOG(INFO) << "read" <<
						": id: " << m_message.hdr.id <<
						", db: " << m_message.hdr.db <<
						", cmd: " << m_message.hdr.cmd <<
						", flags: " << m_message.hdr.flags <<
						", size: " << m_message.hdr.size <<
						std::endl;

					read_header();
				});

	}
};

class server {
public:
	server(io_service_pool& io_pool, const boost::asio::ip::tcp::endpoint &ep)
	: m_io_pool(io_pool)
	, m_acceptor(io_pool.get_service(), ep)
	, m_socket(io_pool.get_service()) {
		start_accept();
	}

private:
	io_service_pool &m_io_pool;
	boost::asio::ip::tcp::acceptor m_acceptor;
	boost::asio::ip::tcp::socket m_socket;

	void start_accept() {
		m_acceptor.async_accept(m_socket,
				[this] (boost::system::error_code ec) {
					if (!ec) {
						connection::pointer new_connection = connection::create(m_io_pool, std::move(m_socket));
						new_connection->start_reading();
					}

					// reschedule acceptor
					start_accept();
				});
	}
};


class db {
public:
	db(uint64_t id) : m_id(id) {}
	db(db &&other)
	: m_id(other.m_id)
	, m_clients(other.m_clients) {
	}

	void connect(connection::pointer client) {
		std::lock_guard<std::mutex> m_guard(m_client_lock);
		m_clients.insert(client);
	}

	void leave(connection::pointer client) {
		std::lock_guard<std::mutex> m_guard(m_client_lock);
		m_clients.erase(client);
	}

private:
	uint64_t m_id;
	std::mutex m_client_lock;
	std::set<connection::pointer> m_clients;
};

typedef std::function<void (scatter::message &)> handler_fn_t;

class node {
public:
	node(handler_fn_t fn) : m_fn(fn) {
		init(1);
	}
	node(const std::string &addr_str, handler_fn_t fn) : m_fn(fn) {
		init(5);

		m_server.reset(new server(*m_io_pool, m_resolver->resolve(addr_str).get()->endpoint()));
	}
	~node() {
	}

	void connect(const std::string &addr, uint64_t db_id) {
		connection::pointer client = connection::create(*m_io_pool);

		LOG(INFO) << "connect: " <<
			": id: " << m_id <<
			", db: " << db_id <<
			std::endl;

		client->connect(m_resolver->resolve(addr).get()).get();

		message msg(0);

		msg.hdr.id = m_id;
		msg.hdr.db = db_id;
		msg.hdr.cmd = SCATTER_CMD_JOIN;
		msg.encode_header();

		LOG(INFO) << "connect: sending join command" <<
			": id: " << m_id <<
			", db: " << db_id <<
			std::endl;

		client->send(msg).get();
		LOG(INFO) << "connect: message has been written, inserting db " << db_id << std::endl;
		auto it = m_dbs.find(db_id);
		if (it == m_dbs.end()) {
			db d(db_id);
			d.connect(client);
			m_dbs.insert(std::pair<uint64_t, db>(db_id, std::move(d)));
		} else {
			it->second.connect(client);
		}

		LOG(INFO) << "connected: " <<
			": id: " << m_id <<
			", db: " << db_id <<
			std::endl;
	}

	int send() {
		return 0;
	}

private:
	handler_fn_t m_fn;

	uint64_t m_id;

	std::unique_ptr<io_service_pool> m_io_pool;
	std::unique_ptr<resolver<>> m_resolver;

	std::unique_ptr<server> m_server;

	std::map<uint64_t, db> m_dbs;

	void init(int io_pool_size) {
		m_id = rand();

		m_io_pool.reset(new io_service_pool(io_pool_size));
		m_resolver.reset(new resolver<>(*m_io_pool));
	}
};

}} // namesapce ioremap::scatter
