add_library(scatter SHARED
	address.cpp
	broadcast.cpp
	connection.cpp
	exception.cpp
	message.cpp
	map.cpp
	node.cpp
	pool.cpp
	route.cpp
	server.cpp
)
target_link_libraries(scatter
	${Boost_LIBRARIES}
	${GLOG_LIBRARIES}
	${MSGPACK_LIBRARIES}
	${RIBOSOME_LIBRARIES}
)
set_target_properties(scatter PROPERTIES
	VERSION ${DEBFULLVERSION}
	SOVERSION ${SCATTER_VERSION_MAJOR}
	LINKER_LANGUAGE CXX
)

add_executable(scatter_server server_main.cpp)
target_link_libraries(scatter_server scatter)

install(TARGETS	scatter_server RUNTIME DESTINATION bin COMPONENT runtime)
install(TARGETS	scatter LIBRARY DESTINATION ${INSTALL_LIB_DIR})
