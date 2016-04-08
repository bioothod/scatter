#include "scatter/server.hpp"

#include <boost/algorithm/string.hpp>
#include <boost/program_options.hpp>

#include <iostream>

using namespace ioremap;

int main(int argc, char *argv[])
{
	namespace bpo = boost::program_options;

	bpo::options_description generic("Scatter generic server options");
	generic.add_options()
		("help", "this help message")
		;

	std::string server_addr;
	std::vector<std::string> remotes;

	bpo::options_description snet("Scatter network options");
	snet.add_options()
		("remote", bpo::value<std::vector<std::string>>(&remotes)->composing(), "remote server nodes to connect: addr:port:family")
		("listen", bpo::value<std::string>(&server_addr)->required(), "listen address: addr:port:family")
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


	srand(time(NULL));

	scatter::server srv(server_addr, 5);

	for (const auto &addr : remotes) {
		auto cn = srv.connect(addr);
		srv.join(cn);
	}

	while (true) {
		sleep(1);
	}
}
