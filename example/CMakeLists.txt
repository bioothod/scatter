add_executable(scatter_client client.cpp)
target_link_libraries(scatter_client
	${Boost_LIBRARIES}
	${FOLLY_LIBRARIES}
	${GLOG_LIBRARIES}
	${WANGLE_LIBRARIES}
	pthread
	)
