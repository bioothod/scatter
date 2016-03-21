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

class service : public wangle::Service<message, message> {
public:
	virtual folly::Future<message> operator()(message msg) override {
		LOG(INFO) << "received message" <<
			": id: " << msg.hdr.id <<
			", db: " << msg.hdr.db <<
			", cmd: " << msg.hdr.cmd <<
			", flags: " << msg.hdr.flags <<
			", size: " << msg.hdr.size <<
			std::endl;

		// process client commands
		if (msg.hdr.cmd >= SCATTER_CMD_CLIENT) {
		}

		// process server commands
	
		message reply(0);
		reply.hdr = msg.hdr;
		reply.size = 0;
		reply.flags = SCATTER_FLAGS_REPLY;
		return reply;
	}

	void transportActive(Context* ctx) override {
		folly::SocketAddress addr;
		ctx->getTransport()->getPeerAddress(&addr);
		LOG(INFO) << "connected peer " << addr.describe() << std::endl;
	}
};

class pipeline_factory : public wangle::PipelineFactory<pipeline_t> {
public:
	pipeline_factory(handler_fn_t fn)
		: wangle::PipelineFactory<pipeline_t>()
		, m_handler(fn) {}

	pipeline_t::Ptr newPipeline(std::shared_ptr<folly::AsyncTransportWrapper> socket) override {
		auto pipeline = pipeline_t::create();
		pipeline->addBack(wangle::AsyncSocketHandler(socket));
		pipeline->addBack(wangle::EventBaseHandler());
		pipeline->addBack(message_codec());
		pipeline->addBack(wangle::MultiplexServerDispatcher<message, message>(&m_service));
		pipeline->finalize();
		return pipeline;
	}

private:
	handler_fn_t m_handler;

	wangle::ExecuterFilter<message, message> m_service {
		std::make_shared<wangle::CPUThreadPoolExecutor>(10),
		std::make_shared<service>()
	};
};

class dispatcher_t : public wangle::ClientDispatcherBase<pipeline_t, message, message> {
public:
	void read(Context* ctx, message msg) override {
		auto req = m_requests.find(msg.id);
		if (req != m_requests.end()) {
			auto p = std::move(req->second);
			m_requests.erase(msg.id);
			p.setValue(in);
		}
	}

	Future<message> operator()(message msg) override {
		auto& p = m_requests[msg.id];
		auto f = p.getFuture();
		p.setInterruptHandler(
			[msg, this] (const folly::exception_wrapper &e) {
				this->m_requests.erase(msg.id);
			});

		this->pipeline_->write(msg);
		return f;
	}

	virtual Future<Unit> close() override {
		printf("Channel closed\n");
		return wangle::ClientDispatcherBase::close();
	}

	virtual Future<Unit> close(Context* ctx) override {
		printf("Channel closed\n");
		return wangle::ClientDispatcherBase::close(ctx);
	}

private:
	std::unordered_map<uint64_t, folly::Promise<message>> m_requests;
};

template <typename Req, typename Resp = Req>
class database_service {
public:
	database_service(std::shared_ptr<Service<Req, Resp>> service)
	: m_service<Req, Resp>(service, std::chrono::seconds(5)) {}
private:
	wangle::ExpiringFilter<Req, Resp> m_service;
};

typedef std::shared_ptr<database_service<message, message>> shared_database_service_t;

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
		generate_id();
		m_client.reset(new wangle::ClientBootstrap<pipeline_t>);
		m_client->group(std::make_shared<wangle::IOThreadPoolExecutor>(1));
		m_client->pipelineFactory(std::make_shared<pipeline_factory>(fn));
	}
	node(const folly::SocketAddress &addr, handler_fn_t fn) {
		generate_id();
		folly::SocketAddress tmp = addr;

		m_server.reset(new wangle::ServerBootstrap<pipeline_t>);
		m_server->childPipeline(std::make_shared<pipeline_factory>(fn));
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
	std::shared_ptr<dispatcher_t> m_dispatcher;

	std::map<uint64_t, db> m_dbs;

	void generate_id() {
		m_id = rand();
	}
};

}} // namesapce ioremap::scatter
