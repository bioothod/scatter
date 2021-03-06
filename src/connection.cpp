#include "scatter/connection.hpp"

#include <msgpack.hpp>

namespace ioremap { namespace scatter {

connection::connection(io_service_pool &io, process_fn_t process, error_fn_t error, proto::socket &&socket)
	: m_pool(io),
	  m_strand(io.get_service()),
	  m_process(process),
	  m_error(error),
	  m_socket(std::move(socket)),
	  m_transactions(0)
{
	set_connection_strings();
}
connection::connection(io_service_pool &io, process_fn_t process, error_fn_t error)
	: m_pool(io),
	  m_strand(io.get_service()),
	  m_process(process),
	  m_error(error),
	  m_socket(io.get_service()),
	  m_transactions(0)
{
}
connection::connection(io_service_pool &io)
	: connection(io, std::bind(&connection::empty_process, this, std::placeholders::_1, std::placeholders::_2),
			std::bind(&connection::empty_error, this, std::placeholders::_1, std::placeholders::_2))
{
}


connection::~connection()
{
	m_expire.stop();

	LOG(INFO) << "connection: " << connection_string() << ": going down";
}


connection::proto::socket& connection::socket()
{
	return m_socket;
}

std::string connection::connection_string() const
{
	return "r:" + m_remote_string + "/l:" + m_local_string;
}
std::string connection::remote_string() const
{
	return m_remote_string;
}
std::string connection::local_string() const
{
	return m_local_string;
}

void connection::close()
{
	// we have to stop expiration thread since it holds callbacks which potentially hold a reference
	// to this connection, thus preventing it from being freed
	//
	// every user of this connection will soon release its reference, and only expiration thread will stuck
	// eventually (when all callbacks have expired) calling destruction of itself from itself
	//
	// if we stop expiration thread here (and clearing its callback list), there will be no users
	// except those who directly hold connection reference, which will be dropped as soon as IO thread
	// reports error and appropriate @error_fn_t is called
	m_expire.stop();
	m_pool.queue_task([this]() { m_socket.close(); });
}

void connection::start_reading()
{
	read_header();
}

void connection::connect(const connection::resolver_iterator it)
{
	std::promise<int> p;
	std::future<int> f = p.get_future();

	LOG(INFO) << "connecting: " << it->endpoint().address().to_string().c_str() << ":" << it->endpoint().port();
	auto self(shared_from_this());
	boost::asio::async_connect(m_socket, it,
			[this, self, &p, &it] (const boost::system::error_code &ec, const connection::resolver_iterator res) {
				(void) res;

				if (ec) {
					LOG(ERROR) << "could not connect to " <<
							it->endpoint().address().to_string().c_str() << ":" << it->endpoint().port() <<
							", error: " << ec.message().c_str();

					p.set_exception(std::make_exception_ptr(create_error(ec.value(), "could not connect to %s:%d: %s",
							it->endpoint().address().to_string().c_str(),
							it->endpoint().port(),
							ec.message().c_str())));
					return;
				}

				set_connection_strings();
				LOG(INFO) << "connected: " << connection_string();

				read_header();
				request_remote_ids(p);
			});
	f.get();
}

void connection::request_remote_ids(std::promise<int> &p)
{
	auto self(shared_from_this());

	send(0, 0, SCATTER_FLAGS_NEED_ACK, SCATTER_CMD_REMOTE_IDS, NULL, 0,
			[this, self, &p] (pointer, message &reply) {
				if (reply.hdr.status) {
					LOG(ERROR) << "connection: " << connection_string() << ", reply: " << reply.to_string() <<
						", could not request remote ids: " << reply.hdr.status;
					p.set_exception(std::make_exception_ptr(create_error(reply.hdr.status,
									"could not request remote ids")));
					return;
				}

				parse_remote_ids(p, reply);
			});
}

// message is already decoded
void connection::parse_remote_ids(std::promise<int> &p, message &msg)
{
	try {
		msgpack::unpacked up;
		msgpack::unpack(&up, msg.data(), msg.hdr.size);

		up.get().convert(&m_cids);
	} catch (const std::exception &e) {
		p.set_exception(std::make_exception_ptr(create_error(-EINVAL, "could not unpack remote ids reply: %s: %s",
						msg.to_string().c_str(), e.what())));
		return;
	}

	VLOG(1) << "connection: " << connection_string() << ", received " << m_cids.size() << " remote ids";
	p.set_value(0);
}

void connection::send(message &msg, connection::process_fn_t complete)
{
	if (msg.hdr.trans == 0)
		msg.hdr.trans = ++m_transactions;
	msg.encode_header();

	VLOG(2) << "connection: " << connection_string() << ", sending data: " << msg.to_string();

	// msg is copied, but it is not very expensive, since data is a shared pointer
	std::shared_ptr<connection::completion_t> c = std::make_shared<connection::completion_t>(shared_from_this(), msg, complete);

	m_strand.post(std::bind(&connection::strand_write_callback, this, c));
}

void connection::send(uint64_t id, uint64_t db, uint64_t flags, int cmd, const char *data, size_t size, connection::process_fn_t complete)
{
	message msg(size);

	if (data && size)
		msg.append(data, size);

	msg.hdr.size = size;
	msg.hdr.id = id;
	msg.hdr.db = db;
	msg.hdr.cmd = cmd;
	msg.hdr.flags = flags;

	send(msg, complete);
}

void connection::send_reply(const message &msg)
{
	message reply;
	reply.hdr = msg.hdr;
	reply.hdr.size = 0;
	reply.hdr.flags = 0;
	reply.hdr.flags = SCATTER_FLAGS_REPLY;

	VLOG(2) << "connection: " << connection_string() << ", sending reply back: " << reply.to_string();

	send(reply, [] (pointer, message &) {});
}

void connection::send_blocked_command(uint64_t id, uint64_t db, int cmd, const char *data, size_t size)
{
	std::promise<int> p;
	std::future<int> f = p.get_future();

	send(id, db, SCATTER_FLAGS_NEED_ACK, cmd, data, size,
		[&p] (pointer self, message &msg) {
			if (msg.hdr.status) {
				LOG(ERROR) << "connection: " << self->connection_string() <<
					", message: " << msg.to_string() <<
					", error: could not send blocked command: " << msg.hdr.status;

				p.set_exception(std::make_exception_ptr(create_error(msg.hdr.status,
							"could not send blocked message: %s, error: %d",
							msg.to_string().c_str(), msg.hdr.status)));
				return;
			}

			p.set_value(msg.hdr.db);
		});

	f.get();
}

void connection::strand_write_callback(shared_completion_t c)
{
	std::unique_lock<std::mutex> guard(m_lock);
	m_outgoing.push_back(c);

	// only put request messages which require acknowledge into the map
	if (c->copy.hdr.flags & SCATTER_FLAGS_NEED_ACK) {
		m_sent[c->copy.hdr.trans] = c;

		auto time = std::chrono::system_clock::now() + std::chrono::seconds(10);
		c->expiration_token = m_expire.insert(time, [c] () {
					VLOG(1) << "connection: " << c->self->connection_string() <<
						", message: " << c->copy.to_string() <<
						", expired";

					c->copy.hdr.status = -ETIMEDOUT;
					c->copy.hdr.size = 0;
					c->copy.hdr.flags = SCATTER_FLAGS_REPLY;

					c->self->process_reply(c->copy);
				});

		VLOG(2) << "connection: " << c->self->connection_string() <<
			", message: " << c->copy.to_string() <<
			", expiration_token: " << c->expiration_token <<
			", added completion callback";
	}

	if (m_outgoing.size() > 1)
		return;

	guard.unlock();

	write_next_buf(c->copy.raw_buffer());
}

void connection::write_next_buf(message::raw_buffer_t buf)
{
	boost::asio::async_write(m_socket, boost::asio::buffer(buf->data(), buf->size()),
			m_strand.wrap(std::bind(&connection::write_completed, this,
					std::placeholders::_1, std::placeholders::_2)));
}

void connection::write_completed(const boost::system::error_code &error, size_t bytes_transferred)
{
	(void) bytes_transferred;

	std::unique_lock<std::mutex> guard(m_lock);
	auto out = m_outgoing.front();

	VLOG(2) << "connection: " << connection_string() <<
		", message: " << out->copy.to_string() <<
		", write completed";

	m_outgoing.pop_front();

	if (!error && !m_outgoing.empty()) {
		message::raw_buffer_t buf = m_outgoing.front()->copy.raw_buffer();
		guard.unlock();

		write_next_buf(buf);
	}
}

void connection::read_header()
{
	auto self(shared_from_this());

	boost::asio::async_read(m_socket,
		boost::asio::buffer(&m_tmp_hdr, message::header_size),
			[this, self] (boost::system::error_code ec, std::size_t /*size*/) {
				if (ec) {
					LOG(INFO) << "connection: " << connection_string() << ", error: " << ec.message();
					// reset connection, drop it from database
					m_error(self, ec);
					return;
				}


				m_tmp_hdr.convert();

				auto msg = std::make_shared<message>(m_tmp_hdr.size);
				msg->hdr = m_tmp_hdr;
				memcpy(msg->buffer(), &m_tmp_hdr, message::header_size);

				read_data(msg);
			});
}

void connection::read_data(std::shared_ptr<message> msg)
{
	auto self(shared_from_this());

	boost::asio::async_read(m_socket,
		boost::asio::buffer(msg->data(), msg->hdr.size),
			[this, self, msg] (boost::system::error_code ec, std::size_t /*size*/) {
				if (ec) {
					LOG(INFO) << "connection: " << connection_string() << ", error: " << ec.message();

					// reset connection, drop it from database
					m_error(self, ec);
					return;
				}
				
				// we have whole message, reschedule read
				// schedule message processing into separate thread pool
				// or process it locally (here)

				VLOG(1) << "connection: " << connection_string() << ", read message: " << msg->to_string();

				process_message(msg);
				read_header();
			});
}

void connection::process_reply(message &msg)
{
	std::unique_lock<std::mutex> guard(m_lock);
	auto p = m_sent.find(msg.hdr.trans);
	if (p == m_sent.end()) {
		LOG(ERROR) << "connection: " << connection_string() <<
			", reply: " << msg.to_string() <<
			", error: there is no handler for reply";

		return;
	}

	shared_completion_t c = p->second;
	m_sent.erase(p);
	auto cb = m_expire.remove(c->expiration_token);
	guard.unlock();

	VLOG(2) << "connection: " << connection_string() <<
		", reply: " << msg.to_string() <<
		", expiration_token: " << c->expiration_token <<
		", removed callback: " << cb.operator bool() << "/" << cb.target_type().name() <<
		", removed completion callback";
	c->complete(shared_from_this(), msg);
	return;
}

void connection::process_message(std::shared_ptr<message> msg)
{
	if (msg->hdr.flags & SCATTER_FLAGS_REPLY) {
		process_reply(*msg);
		return;
	}

	m_process(shared_from_this(), *msg);
	if (msg->hdr.flags & SCATTER_FLAGS_NEED_ACK) {
		send_reply(*msg);
	}
}

const std::vector<connection::cid_t> connection::ids() const
{
	return m_cids;
}
void connection::set_ids(const std::vector<connection::cid_t> &cids)
{
	m_cids = cids;
}

void connection::set_connection_strings()
{
	auto fam = m_socket.local_endpoint().address().is_v4() ? "2" : "10";
	m_local_string = m_socket.local_endpoint().address().to_string() + ":" +
			std::to_string(m_socket.local_endpoint().port()) + ":" + fam;
	m_remote_string = m_socket.remote_endpoint().address().to_string() + ":" +
			std::to_string(m_socket.remote_endpoint().port()) + ":" + fam;

	set_announce_address(address(m_socket.remote_endpoint()));
}

void connection::request_remote_nodes(process_fn_t complete)
{
	message msg;
	msg.hdr.id = m_cids[0];
	msg.hdr.cmd = SCATTER_CMD_CONNECTIONS;
	msg.hdr.flags = SCATTER_FLAGS_NEED_ACK;

	send(msg, complete);
}

void connection::set_announce_address(const address &addr)
{
	m_announce_address = addr;
}
const address &connection::announce_address() const
{
	return m_announce_address;
}

}} // namespace ioremap::scatter

