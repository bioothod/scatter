#include "scatter/scatter.hpp"

using namespace ioremap::scatter;

class routing_handler : public wangle::RoutingDataHandler<std::string> {
public:
	routing_handler(uint64_t connection, wangle::Callback *cb) : wangle:RoutingDataHandler<std::string>(connection, cb) {}

	bool parseRoutingData(folly::IOBufQueue& bqueue, wangle:RoutingData& rdata) override {
		auto info = getContext()->getPipeline()->getTransportInfo();
		const auto& client_addr = info->remoteAddr->getAddressStr();

		LOG(INFO) << "Using client IP " << client_addr
			<< " as routing data to hash to a worker thread";

		rdata.routingData = client_addr;
		rdata.bufQueue.append(bqueue);
		return true;
	}
};

class server_pool : public wangle::ServerPool<std::string> {
public:
	folly::Future<wangle::DefaultPipeline*> connect(wangle::ClientBootstrap<wangle::DefaultPipeline>* client,
			const std::string& routingData) noexcept override {
		folly::SocketAddress address;
		address.setFromLocalPort(FLAGS_upstream_port);

		LOG(INFO) << "Connecting to upstream server " << address
			<< " for subscribing to broadcast";
		return client->connect(address);
	}
};


using client_pipeline = wangle::ObservingPipeline<scatter::message>;

class client_pipeline_factory : public wangle::ObservingPipelineFactory<scatter::message, std::string> {
public:
	client_pipeline_factory(std::shared_ptr<server_pool> spool, std::shared_ptr<node_pipeline_factory> bcast_factory)
		: wangle::ObservingPipelineFactory<scatter::message, std::string>(spool, bcast_factory) {}

	client_pipeline::Ptr newPipeline(std::shared_ptr<folly::AsyncSocket> socket, const std::string& rdata,
			wangle::RoutingDataHandler<std::string>* rhandler,
			std::shared_ptr<folly::TransportInfo> tinfo) override {
		LOG(INFO) << "Creating a new ObservingPipeline for client "
			<< *(tinfo->remoteAddr);

		auto pipeline = client_pipeline::create();
		pipeline->addBack(wangle::AsyncSocketHandler(socket));
		pipeline->addBack(scatter::message_to_byte_encoder());
		pipeline->addBack(std::make_shared<wangle::ObservingHandler<scatter::message, std::string>>(rdata, broadcastPool()));
		pipeline->finalize();
		return pipeline;
	}
};
