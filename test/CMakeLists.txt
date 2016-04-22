add_executable(scatter_test_main main.cpp)
target_link_libraries(scatter_test_main
	${GTEST_LIBRARIES}
	scatter
)

add_executable(scatter_test_dmap dmap.cpp)
target_link_libraries(scatter_test_dmap
	${GTEST_LIBRARIES}
	scatter
)
