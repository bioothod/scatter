#pragma once

#define BOOST_THREAD_PROVIDES_FUTURE
#define BOOST_THREAD_PROVIDES_FUTURE_CONTINUATION
#include <boost/thread/future.hpp>


#include <boost/bind.hpp> // must be first
#include <boost/asio.hpp>

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
		id = folly::Endian::little(id);
		db = folly::Endian::little(db);
		cmd = folly::Endian::little(cmd);
		status = folly::Endian::little(status);
		flags = folly::Endian::little(flags);
		size = folly::Endian::little(size);
	}
};

struct message {
	header			hdr;
	bool			cpu = true;

	// header is prepended to data in this IObuf
	// @hdr points to the start of the buffer
	std::unique_ptr<folly::IOBuf>		buffer;

	message(std::unique_ptr<folly::IOBuf> &&other) {
		buffer = std::move(other);
	}

	message(size_t size) {
		buffer = folly::IOBuf::create(size + sizeof(header));
		buffer->advance(sizeof(header));
	}

	void convert_header() {
		if (!cpu)
			return;

		hdr.convert();
		cpu = false;
	}
};

class tcp_connection : public std::enable_shared_from_this<tcp_connection>
{
public:
	typedef std::shared_ptr<tcp_connection> pointer;

	static pointer create(boost::asio::io_service& io_service) {
		return pointer(new tcp_connection(io_service));
	}

	tcp::socket& socket() {
		return m_socket;
	}

	void start() {
		m_message = make_daytime_string();

		boost::asio::async_write(socket_, boost::asio::buffer(m_message),
				std::bind(&tcp_connection::handle_write, shared_from_this(),
					boost::asio::placeholders::error,
					boost::asio::placeholders::bytes_transferred));
	}

private:
	tcp::socket m_socket;
	std::string m_message;

	tcp_connection(boost::asio::io_service& io_service) : socket_(io_service) {}

	void handle_write(const boost::system::error_code &error, size_t bytes_transferred) {
		(void)error;
		(void)bytes_transferred;
	}
};

class server {
public:
	server(boost::asio::io_service& io_service) : m_acceptor(io_service, tcp::endpoint(tcp::v4(), 13)) {
		start_accept();
	}

private:
	boost::asio::ip::tcp::acceptor m_acceptor;

	void start_accept() {
		tcp_connection::pointer new_connection = tcp_connection::create(m_acceptor.get_io_service());

		m_acceptor.async_accept(new_connection->socket(),
				std::bind(&tcp_server::handle_accept, this, new_connection,
					boost::asio::placeholders::error));
	}

	void handle_accept(tcp_connection::pointer new_connection, const boost::system::error_code& error) {
		if (!error) {
			new_connection->start();
		}

		start_accept();
	}
};

class message_codec: public wangle::Handler<folly::IOBufQueue &, message, message, std::unique_ptr<folly::IOBuf>> {
public:
	typedef typename wangle::Handler<folly::IOBufQueue &, message, message, std::unique_ptr<folly::IOBuf>>::Context Context;

	void read(Context* ctx, folly::IOBufQueue &q) override {
		LOG(INFO) << "codec read: chainLength: " << q.chainLength() << std::endl;

		if (q.chainLength() < sizeof(header))
			return;

		std::unique_ptr<folly::IOBuf> tmp_buf = q.split(sizeof(header));
		tmp_buf->coalesce();

		header *tmp;
		tmp = (header *)tmp_buf->writableData();
		tmp->convert();

		if (q.chainLength() < tmp->size)
			return;

		message msg(q.split(tmp->size));
		msg.hdr = *tmp;

		LOG(INFO) << "codec read: " <<
			": id: " << msg.hdr.id <<
			", db: " << msg.hdr.db <<
			", cmd: " << msg.hdr.cmd <<
			", flags: " << msg.hdr.flags <<
			", size: " << msg.hdr.size <<
			std::endl;

		ctx->fireRead(std::move(msg));
	}

	folly::Future<folly::Unit> write(Context* ctx, message msg) override {
		header *hdr = (header *)msg.buffer->writableBuffer();
		msg.buffer->prepend(sizeof(header));
		*hdr = msg.hdr;
		hdr->convert();

		LOG(INFO) << "codec write: " <<
			": id: " << msg.hdr.id <<
			", db: " << msg.hdr.db <<
			", cmd: " << msg.hdr.cmd <<
			", flags: " << msg.hdr.flags <<
			", size: " << msg.hdr.size <<
			", buffer-size: " << msg.buffer->length() <<
			std::endl;
		return ctx->fireWrite(std::move(msg.buffer));
	}
};

class db {
public:
	db(uint64_t id) : m_id(id) {}
	db(db &&other)
	: m_id(other.m_id)
	, m_pipes(other.m_pipes) {
	}

	void connect(pipeline_t *pipe) {
		auto dispatcher = std::make_shared<dispatcher_t>();
		dispatcher->setPipeline(pipe);

		auto service = std::make_shared<shared_database_service_t>(dispatcher);

		std::lock_guard<std::mutex> m_guard(m_client_lock);
		m_clients.push_back(service);
	}

	folly::Future<message> send(message msg) {
	}

private:
	uint64_t m_id;
	std::mutex m_client_lock;
	std::vector<shared_database_service_t> m_clients;
};

class node {
public:
	node(handler_fn_t fn) {
		init(1);
	}
	node(const std::string &addr_str, handler_fn_t fn) {
		init(5);


	}
	~node() {
	}

	void connect(const folly::SocketAddress &addr, uint64_t db_id) {
		if (!m_client)
			return;

		LOG(INFO) << "connect: " <<
			": id: " << m_id <<
			", db: " << db_id <<
			std::endl;
		m_client->connect(addr)
			.then([&] (pipeline_t *pipe) {
				message msg(0);

				msg.hdr.id = m_id;
				msg.hdr.db = db_id;
				msg.hdr.cmd = SCATTER_CMD_JOIN;

				LOG(INFO) << "connect: sending join command" <<
					": id: " << m_id <<
					", db: " << db_id <<
					std::endl;

				pipe->write(std::move(msg))
					.then([&] () {
						LOG(INFO) << "connect: message has been written, inserting db " << db_id << std::endl;
						auto it = m_dbs.find(db_id);
						if (it == m_dbs.end()) {
							db d(db_id);
							d.connect(std::static_pointer_cast<pipeline_t>(pipe->shared_from_this()));
							m_dbs.insert(std::pair<uint64_t, db>(db_id, std::move(d)));
						} else {
							it->second.connect(std::static_pointer_cast<pipeline_t>(pipe->shared_from_this()));
						}
					});
			}).wait();

		LOG(INFO) << "connected: " <<
			": id: " << m_id <<
			", db: " << db_id <<
			std::endl;
	}

	int send() {
		return 0;
	}

private:
	uint64_t m_id;

	std::unique_ptr<io_service_pool> m_io_pool;
	std::unique_ptr<boost::asio::ip::tcp::resolver> m_resolver;

	std::map<uint64_t, db> m_dbs;

	void init(int io_pool_size) {
		m_id = rand();

		m_io_pool.reset(new io_service_pool(io_pool_size));
		m_resolver.reset(new boost::asio::ip::tcp::resolver(m_io_pool->get_service()));
	}
};

}} // namesapce ioremap::scatter
