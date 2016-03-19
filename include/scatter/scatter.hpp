#pragma once

#include <folly/Bits.h>
#include <folly/io/IOBuf.h>
#include <folly/io/IOBufQueue.h>
#include <folly/SocketAddress.h>


#include <wangle/bootstrap/ClientBootstrap.h>
#include <wangle/bootstrap/ServerBootstrap.h>
#include <wangle/channel/AsyncSocketHandler.h>
#include <wangle/channel/EventBaseHandler.h>
#include <wangle/codec/MessageToByteEncoder.h>
#include <wangle/codec/ByteToMessageDecoder.h>

namespace ioremap { namespace scatter {

enum {
	SCATTER_CMD_SERVER	= 0,
	SCATTER_CMD_JOIN,
	SCATTER_CMD_CLIENT	= 1024,
	__SCATTER_CMD_MAX
};

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

typedef wangle::Pipeline<folly::IOBufQueue &, message> pipeline_t;
typedef std::function<std::unique_ptr<message> (message &)> handler_fn_t;

class server_handler : public wangle::HandlerAdapter<message> {
public:
	virtual void read(Context* ctx, message msg) {
		LOG(INFO) << "received message" <<
			": id: " << msg.hdr.id <<
			", db: " << msg.hdr.db <<
			", cmd: " << msg.hdr.cmd <<
			", flags: " << msg.hdr.flags <<
			", size: " << msg.hdr.size <<
			std::endl;

		// forward client command upstairs
		if (msg.hdr.cmd >= SCATTER_CMD_CLIENT) {
			ctx->fireRead(std::move(msg));
			return;
		}

		// process server commands
	}

	virtual void readException(Context* ctx, folly::exception_wrapper e) override {
		folly::SocketAddress addr;
		ctx->getTransport()->getPeerAddress(&addr);
		LOG(WARNING) << "server: exception received from " << addr.describe() << ": " << folly::exceptionStr(e) << std::endl;
		close(ctx);
	}

	virtual void readEOF(Context* ctx) override {
		folly::SocketAddress addr;
		ctx->getTransport()->getPeerAddress(&addr);
		LOG(INFO) << "server: EOF received from " << addr.describe() << std::endl;
		close(ctx);
	}

	void transportActive(Context* ctx) override {
		folly::SocketAddress addr;
		ctx->getTransport()->getPeerAddress(&addr);
		LOG(INFO) << "connected peer " << addr.describe() << std::endl;
	}
};

class client_handler : public wangle::HandlerAdapter<message> {
public:
	client_handler(handler_fn_t &fn)
		: wangle::HandlerAdapter<message>()
		, m_fn(fn) {
	}

	virtual void read(Context* ctx, message msg) {
		std::unique_ptr<message> reply = m_fn(msg);
		if (reply) {
			write(ctx, std::move(*reply));
		}
	}

	virtual void readException(Context* ctx, folly::exception_wrapper e) override {
		folly::SocketAddress addr;
		ctx->getTransport()->getPeerAddress(&addr);
		LOG(WARNING) << "client: exception received from " << addr.describe() << ": " << folly::exceptionStr(e) << std::endl;
		close(ctx);
	}

	virtual void readEOF(Context* ctx) override {
		folly::SocketAddress addr;
		ctx->getTransport()->getPeerAddress(&addr);
		LOG(INFO) << "client: EOF received from " << addr.describe() << std::endl;
		close(ctx);
	}

private:
	handler_fn_t m_fn;
};


class server_pipeline_factory : public wangle::PipelineFactory<pipeline_t> {
public:
	pipeline_t::Ptr newPipeline(std::shared_ptr<folly::AsyncTransportWrapper> socket) override {
		auto pipeline = pipeline_t::create();
		pipeline->addBack(wangle::AsyncSocketHandler(socket));
		pipeline->addBack(message_codec());
		pipeline->addBack(server_handler());
		pipeline->finalize();
		return pipeline;
	}

private:
};

class client_pipeline_factory : public wangle::PipelineFactory<pipeline_t> {
public:
	client_pipeline_factory(handler_fn_t fn)
		: wangle::PipelineFactory<pipeline_t>()
		, m_handler(fn) {}

	pipeline_t::Ptr newPipeline(std::shared_ptr<folly::AsyncTransportWrapper> socket) override {
		auto pipeline = pipeline_t::create();
		pipeline->addBack(wangle::AsyncSocketHandler(socket));
		pipeline->addBack(wangle::EventBaseHandler());
		pipeline->addBack(message_codec());
		pipeline->addBack(server_handler());
		pipeline->addBack(client_handler(m_handler));
		pipeline->finalize();
		return pipeline;
	}

private:
	handler_fn_t m_handler;
};

class db {
public:
	db(uint64_t id) : m_id(id) {}
	db(db &&other)
	: m_id(other.m_id)
	, m_pipes(other.m_pipes) {
	}

	void connect(pipeline_t::Ptr pipe) {
		std::lock_guard<std::mutex> m_guard(m_pipe_lock);
		m_pipes.push_back(pipe);
	}

private:
	uint64_t m_id;
	std::mutex m_pipe_lock;
	std::vector<pipeline_t::Ptr> m_pipes;
};

class node {
public:
	node(handler_fn_t fn) {
		generate_id();
		m_client.reset(new wangle::ClientBootstrap<pipeline_t>);
		m_client->group(std::make_shared<wangle::IOThreadPoolExecutor>(1));
		m_client->pipelineFactory(std::make_shared<client_pipeline_factory>(fn));
	}
	node(const folly::SocketAddress &addr) {
		generate_id();
		folly::SocketAddress tmp = addr;

		m_server.reset(new wangle::ServerBootstrap<pipeline_t>);
		m_server->childPipeline(std::make_shared<server_pipeline_factory>());
		m_server->bind(tmp);
	}
	~node() {
		if (m_server)
			m_server->stop();
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

	std::unique_ptr<wangle::ServerBootstrap<pipeline_t>> m_server;
	std::unique_ptr<wangle::ClientBootstrap<pipeline_t>> m_client;
	std::map<uint64_t, db> m_dbs;

	void generate_id() {
		m_id = rand();
	}
};

}} // namesapce ioremap::scatter
