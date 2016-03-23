#include "scatter/exception.hpp"

#include <cstdarg>
#include <cstdio>
#include <sstream>

#include <errno.h>

namespace ioremap { namespace scatter {

error::error(int code, const std::string &message) throw() : m_errno(code), m_message(message)
{
}

int error::error_code() const
{
	return m_errno;
}

const char *error::what() const throw()
{
	return m_message.c_str();
}

std::string error::error_message() const throw()
{
	return m_message;
}

not_found_error::not_found_error(const std::string &message) throw()
	: error(-ENOENT, message)
{
}

timeout_error::timeout_error(const std::string &message) throw()
	: error(-ETIMEDOUT, message)
{
}

no_such_address_error::no_such_address_error(const std::string &message) throw()
	: error(-ENXIO, message)
{
}

void error_info::throw_error() const
{
	switch (m_code) {
		case -ENOENT:
			throw not_found_error(m_message);
			break;
		case -ETIMEDOUT:
			throw timeout_error(m_message);
			break;
		case -ENOMEM:
			throw std::bad_alloc();
			break;
		case -ENXIO:
			throw no_such_address_error(m_message);
			break;
		case 0:
			// Do nothing, it's not an error
			break;
		default:
			throw error(m_code, m_message);
			break;
	}
}

static error_info create_info(int err, const char *format, va_list args)
{
	if (err == -ENOMEM)
		return error_info(err, std::string());

	char buffer[1024];
	const size_t buffer_size = sizeof(buffer);
	vsnprintf(buffer, buffer_size, format, args);
	buffer[buffer_size - 1] = '\0';
	return error_info(err, std::string(buffer, buffer_size));
}


void throw_error(int err, const char *format, ...)
{
	va_list args;
	va_start(args, format);
	error_info error = create_info(err, format, args);
	va_end(args);
	error.throw_error();
}

error_info create_error_info(int err, const char *format, ...)
{
	va_list args;
	va_start(args, format);
	error_info error = create_info(err, format, args);
	va_end(args);
	return error;
}

error create_error(int err, const char *format, ...)
{
	va_list args;
	va_start(args, format);
	error_info einfo = create_info(err, format, args);
	va_end(args);
	return error(err, einfo.message());
}


} } // namespace ioremap::elliptics

