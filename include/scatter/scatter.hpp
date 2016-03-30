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
#include <unordered_map>

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
				{ SCATTER_CMD_SERVER, "[server]" },
				{ SCATTER_CMD_JOIN, "[join]" },
				{ SCATTER_CMD_CLIENT, "[client]" },
			};

			return command_strings[cmd];
		}

		return "[unsupported]";
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
	// this member is always in CPU byte order
	// data in @buffer() can be either in CPU or LE order
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
		convert_header(&hdr);
		return true;
	}
	bool encode_header() {
		memcpy(buffer(), &hdr, sizeof(header));
		convert_header((header *)buffer());
		return true;
	}

	uint64_t id() const {
		return hdr.id;
	}

	uint64_t flags() const {
		return hdr.flags;
	}

	uint64_t db() const {
		return hdr.db;
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

	void append(const char *ptr, size_t size) {
		if (size > total_size() - headroom()) {
			throw_error(-EINVAL, "append: data_offset: %zd, buffer_size: %zd, available: %zd, size: %zd, "
						"error: not enough size in data area",
					m_data_offset, m_buffer->size(), total_size() - headroom(), size);
		}

		memcpy(data(), ptr, size);
		m_data_offset += size;
	}

	typedef std::shared_ptr<std::vector<char>> raw_buffer_t;
	raw_buffer_t raw_buffer() const {
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
		ss <<	"[id: " << hdr.id <<
			", db: " << hdr.db <<
			", status: " << hdr.status <<
			", cmd: " << hdr.cmd << " " << hdr.cmd_string() <<
			", flags: 0x" << std::hex << hdr.flags << std::dec << " " << hdr.flags_string() <<
			", size: " << hdr.size <<
			"]";

		return ss.str();
	}

private:
	// this buffer has to have enough size to host header and data
	raw_buffer_t m_buffer;

	bool m_cpu = true;
	size_t m_data_offset = 0;

	void convert_header(header *h) {
		if (!m_cpu)
			return;

		h->convert();
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

	std::string connection_string() const {
		return "r:" + m_remote_string + "/l:" + m_local_string;
	}

	void close() {
		m_pool.queue_task([this]() { m_socket.close(); });
	}

	void start_reading() {
		read_header();
	}

	void connect(const resolver_iterator it) {
		std::promise<int> p;
		std::future<int> f = p.get_future();

		auto self(shared_from_this());
		boost::asio::async_connect(m_socket, it, [this, self, &p] (const boost::system::error_code &ec, const resolver_iterator it) {
					if (ec) {
						throw_error(ec.value(), "could not connect to %s:%d: %s",
								it->endpoint().address().to_string().c_str(),
								it->endpoint().port(),
								ec.message().c_str());
					}

					m_local_string = m_socket.local_endpoint().address().to_string() + ":" +
						std::to_string(m_socket.local_endpoint().port());
					m_remote_string = m_socket.remote_endpoint().address().to_string() + ":" +
						std::to_string(m_socket.remote_endpoint().port());
					p.set_value(0);

					read_header();
				});

		f.wait();
	}

	// message has to be already encoded
	void send(const message &msg, handler_fn_t fn) {
		auto buf = msg.raw_buffer();
		uint64_t id = msg.id();
		uint64_t flags = msg.flags();

		m_strand.post(std::bind(&connection::strand_write_callback, this, id, flags, buf, fn));
	}

	void send_reply(const message &msg) {
		auto self(shared_from_this());

		std::shared_ptr<message> reply = std::make_shared<message>();
		reply->hdr = msg.hdr;
		reply->hdr.size = 0;
		reply->hdr.flags &= ~SCATTER_FLAGS_NEED_ACK;
		reply->hdr.flags |= SCATTER_FLAGS_REPLY;
		reply->encode_header();

		send(*reply, [this, self, reply] (pointer, message &) {
				});
	}

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

	connection(io_service_pool &io, handler_fn_t fn, typename proto::socket &&socket)
		: m_pool(io),
		  m_strand(io.get_service()),
		  m_fn(fn),
		  m_socket(std::move(socket)) {
		m_local_string = m_socket.local_endpoint().address().to_string() + ":" + std::to_string(m_socket.local_endpoint().port());
		m_remote_string = m_socket.remote_endpoint().address().to_string() + ":" + std::to_string(m_socket.remote_endpoint().port());
	}
	connection(io_service_pool &io, handler_fn_t fn)
		: m_pool(io),
		  m_strand(io.get_service()),
		  m_fn(fn),
		  m_socket(io.get_service()) {
	}

	void strand_write_callback(uint64_t id, uint64_t flags, message::raw_buffer_t buf, handler_fn_t fn) {
		completion_t cmpl{ id, flags, buf, fn };

		std::unique_lock<std::mutex> guard(m_lock);
		m_outgoing.push_back(cmpl);

		// only put request messages which require acknowledge into the map
		if (flags & SCATTER_FLAGS_NEED_ACK) {
			m_sent[id] = cmpl;

			LOG(INFO) << "connection: " << connection_string() <<
				", id: " << id <<
				", added completion callback";
		}

		if (m_outgoing.size() > 1)
			return;

		guard.unlock();

		write_next_buf(buf);
	}

	void write_next_buf(message::raw_buffer_t buf) {
		boost::asio::async_write(m_socket, boost::asio::buffer(buf->data(), buf->size()),
				m_strand.wrap(std::bind(&connection::write_completed, this,
						std::placeholders::_1, std::placeholders::_2)));
	}

	void write_completed(const boost::system::error_code &error, size_t bytes_transferred) {
		(void) bytes_transferred;

		std::unique_lock<std::mutex> guard(m_lock);
		m_outgoing.pop_front();

		if (!error && !m_outgoing.empty()) {
			uint64_t id = m_outgoing[0].id;

			auto p = m_sent.find(id);
			if (p == m_sent.end()) {
				return;
			}

			auto &c = p->second;
			guard.unlock();

			write_next_buf(c.buf);
		}
	}

	void read_header() {
		auto self(shared_from_this());

		boost::asio::async_read(m_socket,
			boost::asio::buffer(m_message.buffer(), message::header_size),
				[this, self] (boost::system::error_code ec, std::size_t /*size*/) {
					if (ec || !m_message.decode_header()) {
						LOG(ERROR) << "connection: " << connection_string() << ": error: " << ec.message();
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
						LOG(ERROR) << "connection: " << connection_string() << ": error: " << ec.message();
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

					LOG(INFO) << "read message: " << m_message.to_string();

					process_message();
					read_header();
				});
	}

	void process_message() {
		if (m_message.hdr.flags & SCATTER_FLAGS_REPLY) {
			uint64_t id = m_message.id();

			std::unique_lock<std::mutex> guard(m_lock);
			auto p = m_sent.find(id);
			if (p == m_sent.end()) {
				LOG(ERROR) << "connection: " << m_remote_string << "->" << m_local_string <<
					", message: " << m_message.to_string() <<
					", error: there is no handler for reply";

				return;
			}

			auto c = std::move(p->second);
			m_sent.erase(id);
			guard.unlock();

			LOG(INFO) << "connection: " << connection_string() <<
				", id: " << id <<
				", removed completion callback";
			c.complete(shared_from_this(), m_message);
			return;
		}

		m_fn(shared_from_this(), m_message);
		if (m_message.hdr.flags & SCATTER_FLAGS_NEED_ACK) {
			send_reply(m_message);
		}
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
		LOG(INFO) << "db: " << m_id <<
				", connection: " << client->connection_string() <<
				", command: join";
	}

	void leave(connection::pointer client) {
		std::lock_guard<std::mutex> m_guard(m_lock);
		m_clients.erase(client);

		LOG(INFO) << "db: " << m_id <<
				", connection: " << client->connection_string() <<
				", command: leave";
	}

	void send(message &msg, connection::handler_fn_t complete) {
		send(std::shared_ptr<connection>(), msg, complete);
	}

	// message must be already encoded
	void send(connection::pointer self, message &msg, connection::handler_fn_t complete) {
		std::unique_lock<std::mutex> guard(m_lock);
		std::vector<connection::pointer> copy(m_clients.begin(), m_clients.end());
		guard.unlock();

		std::atomic_int completed(copy.size());
		for (auto &c : copy) {
			LOG(INFO) << "db: " << m_id <<
				": broadcasting message: " << msg.to_string() <<
				", connection: " << c->connection_string() <<
				", completed: " << completed << "/" << copy.size();

			if (self && (c->socket().local_endpoint() == self->socket().local_endpoint()) &&
					(c->socket().remote_endpoint() == self->socket().remote_endpoint())) {
				if (--completed == 0) {
					complete(self, msg);
				}
				continue;
			}

			auto buf = msg.raw_buffer();
			c->send(msg, [this, buf, self, complete, &completed] (connection::pointer fwd, message &reply) {
						if (reply.hdr.status) {
							leave(fwd);
						}

						if (--completed == 0) {
							complete(self, reply);
						}
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

		LOG(INFO) << "joining: " <<
			": id: " << m_id <<
			", db: " << db_id <<
			std::endl;

		msg.encode_header();

		std::promise<int> p;
		std::future<int> f = p.get_future();

		client->send(msg,
			[&] (scatter::connection::pointer self, scatter::message &msg) {
				if (msg.hdr.status) {
					throw_error(msg.hdr.status, "could not join database id: %ld, error: %d", db_id, msg.hdr.status);
				}

				std::unique_lock<std::mutex> guard(m_lock);
				db::create_and_insert(m_dbs, db_id, client);
				guard.unlock();

				LOG(INFO) << "joined: " <<
					": id: " << m_id <<
					", db: " << db_id <<
					std::endl;

				p.set_value(db_id);
			});

		f.wait();
	}

	// message should not be encoded
	void send(message &msg, connection::handler_fn_t complete) {
		long db = msg.hdr.db;

		msg.encode_header();

		std::unique_lock<std::mutex> guard(m_lock);
		auto it = m_dbs.find(db);
		if (it == m_dbs.end()) {
			throw_error(-ENOENT, "node didn't join to database %ld", db);
		}

		it->second.send(msg, complete);
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
