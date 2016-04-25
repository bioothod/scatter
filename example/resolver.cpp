#include "scatter/pool.hpp"
#include "scatter/resolver.hpp"

#include <boost/program_options.hpp>

#include <iostream>

using namespace ioremap;

int main(int argc, char *argv[])
{
	namespace bpo = boost::program_options;

	bpo::options_description generic("Scatter generic example options");
	generic.add_options()
		("help", "this help message")
		;

	std::vector<std::string> remotes;

	bpo::options_description snet("Scatter network options");
	snet.add_options()
		("remote", bpo::value<std::vector<std::string>>(&remotes)->composing(), "remote node: addr:port:family")
		;

	bpo::options_description cmdline_options;
	cmdline_options.add(generic).add(snet);

	bpo::variables_map vm;

	try {
		bpo::store(bpo::command_line_parser(argc, argv).options(cmdline_options).run(), vm);

		if (vm.count("help")) {
			std::cout << cmdline_options << std::endl;
			return 0;
		}

		bpo::notify(vm);
	} catch (const std::exception &e) {
		std::cerr << "Invalid options: " << e.what() << "\n" << cmdline_options << std::endl;
		return -1;
	}

	scatter::io_service_pool io(3);
	scatter::resolver<> res(io);

	std::vector<boost::asio::ip::tcp::resolver::iterator> afs;
	for (const auto &addr : remotes) {
		afs.emplace_back(std::move(res.resolve(addr)));
	}

	int pos = 0;
	for (auto &f: afs) {
		try {
			auto endpoint = f->endpoint();
			auto addr = endpoint.address();
			auto port = endpoint.port();
			std::cout << remotes[pos] << " : " << addr.to_string() << ":" << port << std::endl;
		} catch (const std::exception &e) {
			std::cout << remotes[pos] << " : " << "could not resolve address: " << e.what() << std::endl;
		}
		pos++;
	}
}
