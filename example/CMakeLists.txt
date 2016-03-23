add_executable(scatter_client client.cpp)
target_link_libraries(scatter_client
	${Boost_LIBRARIES}
	${GLOG_LIBRARIES}
	scatter
)

add_executable(scatter_resolver resolver.cpp)
target_link_libraries(scatter_resolver
	${Boost_LIBRARIES}
	${GLOG_LIBRARIES}
	scatter
)
