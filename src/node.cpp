#include "scatter/node.hpp"

namespace ioremap { namespace scatter {

node::node()
{
	init(1);
}
node::node(const std::string &addr_str)
{
	init(5);

	m_server.reset(new server(*m_io_pool, m_resolver->resolve(addr_str).get()->endpoint()));
}
node::~node()
{
}

connection::pointer node::connect(const std::string &addr, typename connection::process_fn_t process)
{
	connection::pointer client = connection::create(*m_io_pool, process,
			std::bind(&node::drop, this, std::placeholders::_1, std::placeholders::_2));

	LOG(INFO) << "connecting to addr: " << addr << ", id: " << m_id;

	client->connect(m_resolver->resolve(addr).get());

	LOG(INFO) << "connected to addr: " << addr << ", id: " << m_id;
	return client;
}

void node::join(connection::pointer cn, uint64_t db)
{
	message msg(0);

	msg.hdr.id = m_id;
	msg.hdr.db = db;
	msg.hdr.cmd = SCATTER_CMD_JOIN;
	msg.hdr.flags = SCATTER_FLAGS_NEED_ACK;

	LOG(INFO) << "joining: " <<
		": id: " << m_id <<
		", db: " << db <<
		std::endl;

	msg.encode_header();

	std::promise<int> p;
	std::future<int> f = p.get_future();

	cn->send(msg,
		[&] (scatter::connection::pointer self, scatter::message &msg) {
			if (msg.hdr.status) {
				throw_error(msg.hdr.status, "could not join database id: %ld, error: %d", db, msg.hdr.status);
			}

			std::unique_lock<std::mutex> guard(m_lock);
			broadcast::create_and_insert(m_bcast, db, cn);
			guard.unlock();

			LOG(INFO) << "joined: " <<
				": id: " << m_id <<
				", db: " << db <<
				std::endl;

			p.set_value(db);
		});

	f.wait();
}

void node::drop(connection::pointer cn, const boost::system::error_code &ec)
{
	std::unique_lock<std::mutex> guard(m_lock);
	for (auto &p : m_bcast) {
		broadcast &bcast = p.second;

		bcast.leave(cn);
	}
}

// message should be encoded
void node::send(message &msg, connection::process_fn_t complete)
{
	long db = msg.db();

	std::unique_lock<std::mutex> guard(m_lock);
	auto it = m_bcast.find(db);
	if (it == m_bcast.end()) {
		throw_error(-ENOENT, "node didn't join to database %ld", db);
	}

	it->second.send(msg, complete);
}

void node::init(int io_pool_size)
{
	m_id = rand();

	m_io_pool.reset(new io_service_pool(io_pool_size));
	m_resolver.reset(new resolver<>(*m_io_pool));
}

}} // namespace ioremap::scatter
