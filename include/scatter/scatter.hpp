#pragma once

#include "scatter/endian.hpp"
#include "scatter/exception.hpp"
#include "scatter/pool.hpp"
#include "scatter/resolver.hpp"

#include <boost/bind.hpp> // must be first
#include <boost/asio.hpp>
#include <boost/asio/use_future.hpp>

#include <glog/logging.h>

#include <deque>
#include <set>

namespace ioremap { namespace scatter {

enum {
	SCATTER_CMD_SERVER	= 0,
	SCATTER_CMD_JOIN,
	SCATTER_CMD_CLIENT	= 1024,
	__SCATTER_CMD_MAX
};

// this message is a reply
#define SCATTER_FLAGS_REPLY		(1<<0)

// send acknowledge to given request
#define SCATTER_FLAGS_NEED_ACK		(1<<1)

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

	std::string cmd_string() const {
		if (cmd >= 0 && cmd < __SCATTER_CMD_MAX) {
			static std::map<int, std::string> command_strings = {
				{ SCATTER_CMD_SERVER, "server" },
				{ SCATTER_CMD_JOIN, "join" },
				{ SCATTER_CMD_CLIENT, "client" },
			};

			return command_strings[cmd];
		}

		return "unsupported";
	}

	std::string flags_string() const {
		static struct flag_info {
			uint64_t flag;
			const char *name;
		} infos[] = {
			{ SCATTER_FLAGS_REPLY, "reply" },
			{ SCATTER_FLAGS_NEED_ACK, "need_ack" },
		};

		std::ostringstream ss;
		ss << "[";
		std::string prefix = "";
		for (size_t i = 0; i < sizeof(infos) / sizeof(infos[0]); ++i) {
			if (flags & infos[i].flag) {
				ss << prefix << infos[i].name;
				prefix = "|";
			}
		}
		ss << "]";
		return ss.str();
	}
};

class message {
public:
	header			hdr;

	enum {
		header_size = sizeof(header)
	};

	message() : message(0) {
	}

	message(size_t size) {
		m_buffer = std::make_shared<std::vector<char>>(size + sizeof(header));
		advance(sizeof(header));
	}

	message(std::shared_ptr<std::vector<char>> raw_buffer) : m_buffer(raw_buffer) {
	}

	message(const message &other) : m_buffer(other.m_buffer) {
		hdr = other.hdr;
		m_cpu = other.m_cpu;
		m_data_offset = other.m_data_offset;
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

	std::shared_ptr<std::vector<char>> raw_buffer() {
		return m_buffer;
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

	std::string to_string() const {
		std::ostringstream ss;
		ss <<	": id: " << hdr.id <<
			", db: " << hdr.db <<
			", cmd: " << hdr.cmd << " " << hdr.cmd_string() <<
			", flags: 0x" << std::hex << hdr.flags << std::dec << " " << hdr.flags_string() <<
			", size: " << hdr.size;

		return ss.str();
	}

private:
	// this buffer has to have enough size to host header and data
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
	typedef std::function<void (const boost::system::error_code &, size_t)> handler_t;

	typedef std::function<void (pointer client, message &)> handler_fn_t;

	static pointer create(io_service_pool& io_pool, handler_fn_t fn, typename proto::socket &&socket) {
		return pointer(new connection(io_pool, fn, std::move(socket)));
	}
	static pointer create(io_service_pool& io_pool, handler_fn_t fn) {
		return pointer(new connection(io_pool, fn));
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

	void connect(const resolver_iterator it) {
		return boost::asio::async_connect(m_socket, it, std::bind(&connection::read_header, shared_from_this()));
	}

	void send(const message &msg, handler_t fn) {
		m_strand.post(std::bind(&connection::strand_write_callback, this, msg, fn));
	}

	void send_reply(const message &msg) {
		auto self(shared_from_this());

		std::shared_ptr<message> reply = std::make_shared<message>();
		reply->hdr = msg.hdr;
		reply->hdr.size = 0;
		reply->hdr.flags &= ~SCATTER_FLAGS_NEED_ACK;
		reply->hdr.flags |= SCATTER_FLAGS_REPLY;
		reply->encode_header();

		send(*reply, [this, self, reply] (const boost::system::error_code /*ec*/, std::size_t /*size*/) {
				});
	}

private:
	io_service_pool &m_pool;
	boost::asio::io_service::strand m_strand;
	handler_fn_t m_fn;
	proto::socket m_socket;
	message m_message;

	std::deque<std::pair<message, handler_t>> m_outgoing;

	connection(io_service_pool &io, handler_fn_t fn, typename proto::socket &&socket)
		: m_pool(io),
		  m_strand(io.get_service()),
		  m_fn(fn),
		  m_socket(std::move(socket)) {}
	connection(io_service_pool &io, handler_fn_t fn)
		: m_pool(io),
		  m_strand(io.get_service()),
		  m_fn(fn),
		  m_socket(m_pool.get_service()) {}

	void strand_write_callback(const message &msg, handler_t fn) {
		m_outgoing.push_back(std::pair<message, handler_t>(msg, fn));
		if (m_outgoing.size() > 1)
			return;

		write_next();
	}

	void write_next() {
		auto &p = m_outgoing[0];
		message &msg = p.first;
		handler_t &fn = p.second;

		boost::asio::async_write(m_socket, boost::asio::buffer(msg.buffer(), msg.total_size()),
				m_strand.wrap(std::bind(&connection::handle_write, this, std::ref(fn),
						std::placeholders::_1, std::placeholders::_2)));
	}

	void handle_write(handler_t &fn, const boost::system::error_code &error, size_t bytes_transferred) {
		fn(error, bytes_transferred);

		m_outgoing.pop_front();

		if (!error && !m_outgoing.empty()) {
			write_next();
		}
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

					LOG(INFO) << "read message " << m_message.to_string();

					process_message();
					read_header();
				});
	}

	void process_message() {
		m_fn(shared_from_this(), m_message);
	}
};

class db {
public:
	db(uint64_t id) : m_id(id) {}
	db(db &&other)
	: m_id(other.m_id)
	, m_clients(other.m_clients) {
	}

	template <typename C>
	static void create_and_insert(C &collection, uint64_t id, connection::pointer client) {
		auto db_it = collection.find(id);
		if (db_it == collection.end()) {
			db d(id);
			d.join(client);
			collection.insert(std::pair<uint64_t, db>(id, std::move(d)));
		} else {
			db_it->second.join(client);
		}
	}

	void join(connection::pointer client) {
		std::lock_guard<std::mutex> m_guard(m_lock);
		m_clients.insert(client);
	}

	void leave(connection::pointer client) {
		std::lock_guard<std::mutex> m_guard(m_lock);
		m_clients.erase(client);
	}

	// message must be already encoded
	void send(connection::pointer self, message &msg, connection::handler_t complete) {
		std::unique_lock<std::mutex> guard(m_lock);
		std::vector<connection::pointer> copy(m_clients.begin(), m_clients.end());
		guard.unlock();

		for (auto &c : copy) {
			if (c->socket().local_endpoint() == self->socket().local_endpoint())
				continue;

			auto buf = msg.raw_buffer();
			c->send(msg, [this, buf, complete] (const boost::system::error_code &error, size_t size) {
						complete(error, size);
					});
		}
	}

private:
	uint64_t m_id;

	std::mutex m_lock;
	std::set<connection::pointer> m_clients;
	std::deque<message> m_log;
};


class server {
public:
	typedef typename boost::asio::ip::tcp proto;

	server(io_service_pool& io_pool, const boost::asio::ip::tcp::endpoint &ep)
	: m_io_pool(io_pool),
	  m_acceptor(io_pool.get_service(), ep),
	  m_socket(io_pool.get_service()) {
		start_accept();
	}

private:
	io_service_pool &m_io_pool;
	proto::acceptor m_acceptor;
	proto::socket m_socket;

	std::mutex m_lock;
	std::map<uint64_t, db> m_dbs;
	std::map<typename proto::endpoint, connection::pointer> m_connected;

	void forward_message(connection::pointer client, message &msg) {
	}

	void message_handler(connection::pointer client, message &msg) {
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

		if (msg.hdr.flags & SCATTER_FLAGS_NEED_ACK) {
			client->send_reply(msg);
		}
	}

	void start_accept() {
		m_acceptor.async_accept(m_socket,
				[this] (boost::system::error_code ec) {
					if (!ec) {
						connection::pointer client = connection::create(m_io_pool,
								std::bind(&server::message_handler, this,
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
};

class node {
public:
	node() {
		init(1);
	}
	node(const std::string &addr_str) {
		init(5);

		m_server.reset(new server(*m_io_pool, m_resolver->resolve(addr_str).get()->endpoint()));
	}
	~node() {
	}

	connection::pointer connect(const std::string &addr, typename connection::handler_fn_t fn) {
		connection::pointer client = connection::create(*m_io_pool, fn);

		LOG(INFO) << "connecting to addr: " << addr << ", id: " << m_id;

		client->connect(m_resolver->resolve(addr).get());

		LOG(INFO) << "connected to addr: " << addr << ", id: " << m_id;
		return client;
	}

	void join(connection::pointer client, uint64_t db_id) {
		message msg(0);

		msg.hdr.id = m_id;
		msg.hdr.db = db_id;
		msg.hdr.cmd = SCATTER_CMD_JOIN;
		msg.hdr.flags = SCATTER_FLAGS_NEED_ACK;
		msg.encode_header();

		LOG(INFO) << "joining: " <<
			": id: " << m_id <<
			", db: " << db_id <<
			std::endl;

		client->send(msg,
			[&] (const boost::system::error_code &ec, size_t) {
				if (ec) {
					throw_error(ec.value(), "could not join to database id: %ld, error: %s", db_id, ec.message().c_str());
				}

				std::unique_lock<std::mutex> guard(m_lock);
				db::create_and_insert(m_dbs, db_id, client);
				guard.unlock();

				LOG(INFO) << "joined: " <<
					": id: " << m_id <<
					", db: " << db_id <<
					std::endl;
			});


	}

	int send() {
		return 0;
	}

private:
	uint64_t m_id;

	std::unique_ptr<io_service_pool> m_io_pool;
	std::unique_ptr<resolver<>> m_resolver;

	std::mutex m_lock;
	std::unique_ptr<server> m_server;

	std::map<uint64_t, db> m_dbs;

	void init(int io_pool_size) {
		m_id = rand();

		m_io_pool.reset(new io_service_pool(io_pool_size));
		m_resolver.reset(new resolver<>(*m_io_pool));
	}
};

}} // namesapce ioremap::scatter
