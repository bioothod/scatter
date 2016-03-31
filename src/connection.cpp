#include "scatter/connection.hpp"

namespace ioremap { namespace scatter {

connection::proto::socket& connection::socket()
{
	return m_socket;
}

std::string connection::connection_string() const
{
	return "r:" + m_remote_string + "/l:" + m_local_string;
}

void connection::close()
{
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

	auto self(shared_from_this());
	boost::asio::async_connect(m_socket, it,
			[this, self, &p] (const boost::system::error_code &ec, const connection::resolver_iterator it) {
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
void connection::send(const message &msg, connection::handler_fn_t fn)
{
	auto buf = msg.raw_buffer();
	uint64_t id = msg.id();
	uint64_t flags = msg.flags();

	m_strand.post(std::bind(&connection::strand_write_callback, this, id, flags, buf, fn));
}

void connection::send_reply(const message &msg)
{
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

connection::connection(io_service_pool &io, connection::handler_fn_t fn, typename connection::proto::socket &&socket)
	: m_pool(io),
	  m_strand(io.get_service()),
	  m_fn(fn),
	  m_socket(std::move(socket))
{
	m_local_string = m_socket.local_endpoint().address().to_string() + ":" + std::to_string(m_socket.local_endpoint().port());
	m_remote_string = m_socket.remote_endpoint().address().to_string() + ":" + std::to_string(m_socket.remote_endpoint().port());
}
connection::connection(io_service_pool &io, connection::handler_fn_t fn)
	: m_pool(io),
	  m_strand(io.get_service()),
	  m_fn(fn),
	  m_socket(io.get_service())
{
}

void connection::strand_write_callback(uint64_t id, uint64_t flags, message::raw_buffer_t buf, connection::handler_fn_t fn)
{
	connection::completion_t cmpl{ id, flags, buf, fn };

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

void connection::read_header()
{
	auto self(shared_from_this());

	boost::asio::async_read(m_socket,
		boost::asio::buffer(m_message.buffer(), message::header_size),
			[this, self] (boost::system::error_code ec, std::size_t /*size*/) {
				if (ec || !m_message.decode_header()) {
					LOG(ERROR) << "connection: " << connection_string() << ", error: " << ec.message();
					// reset connection, drop it from database
					return;
				}

				read_data();
			});
}

void connection::read_data()
{
	auto self(shared_from_this());

	m_message.resize(m_message.hdr.size);
	boost::asio::async_read(m_socket,
		boost::asio::buffer(m_message.data(), m_message.hdr.size),
			[this, self] (boost::system::error_code ec, std::size_t /*size*/) {
				if (ec) {
					LOG(ERROR) << "connection: " << connection_string() << ", error: " << ec.message();

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

				LOG(INFO) << "connection: " << connection_string() << ", read message: " << m_message.to_string();

				process_message();
				read_header();
			});
}

void connection::process_message()
{
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

}} // namespace ioremap::scatter

