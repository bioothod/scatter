project(scatter)
cmake_minimum_required(VERSION 2.6)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -W -Wall -Wextra -fstack-protector-all -g")

file(READ "${CMAKE_CURRENT_SOURCE_DIR}/debian/changelog" DEBCHANGELOG)

# version string: 0.3.12
# scatter major 0
# scatter minor 3.12
string(REGEX MATCH "([0-9]+\\.[0-9]+\\.[0-9]+)" DEBFULLVERSION "${DEBCHANGELOG}")
string(REGEX MATCH "([0-9]+)" SCATTER_VERSION_MAJOR "${DEBFULLVERSION}")
string(REGEX MATCH "([0-9]+\\.[0-9]+$)" SCATTER_VERSION_MINOR "${DEBFULLVERSION}")

include(TestBigEndian)
include(cmake/locate_library.cmake)

LOCATE_LIBRARY(MSGPACK "msgpack.hpp" "msgpack")
LOCATE_LIBRARY(GLOG "glog/logging.h" "glog")

find_package(Boost COMPONENTS system thread program_options REQUIRED)
find_package(Ribosome REQUIRED)
#find_package(Elliptics REQUIRED)

# Test endianness
test_big_endian(HAVE_BIG_ENDIAN)
if(HAVE_BIG_ENDIAN)
    add_definitions(-DBYTEORDER=4321)
    add_definitions(-DWORDS_BIGENDIAN=1)
else()
    add_definitions(-DBYTEORDER=1234)
endif()

set(CMAKE_EXE_LINKER_FLAGS -pthread)
find_package(Threads REQUIRED)

get_property(LIB64 GLOBAL PROPERTY FIND_LIBRARY_USE_LIB64_PATHS)
if ("${LIB64}" STREQUAL "TRUE")
    set(LIB_SUFFIX 64)
else()
    set(LIB_SUFFIX "")
endif()

set(INSTALL_LIB_DIR lib${LIB_SUFFIX} CACHE PATH "Installation directory for libraries")
mark_as_advanced(INSTALL_LIB_DIR)


option(WANT_GTEST "Disable if there is no way to download Google test suite" ON)

if (WANT_GTEST)
	include(ExternalProject)
	ExternalProject_Add(googletest
		URL https://github.com/google/googletest/archive/release-1.7.0.tar.gz
		PREFIX "test/googletest-release-1.7.0"
		INSTALL_COMMAND ""
		LOG_DOWNLOAD ON
		LOG_CONFIGURE ON
		LOG_BUILD ON
	)

	ExternalProject_Get_Property(googletest source_dir)
	set(GTEST_INCLUDE_DIRS ${source_dir}/include)

	ExternalProject_Get_Property(googletest binary_dir)
	set(GTEST_LIBRARY_PATH ${binary_dir}/${CMAKE_FIND_LIBRARY_PREFIXES}gtest.a)
	set(GTEST_LIBRARIES gtest)
	add_library(${GTEST_LIBRARIES} UNKNOWN IMPORTED)
	set_property(TARGET ${GTEST_LIBRARIES} PROPERTY IMPORTED_LOCATION ${GTEST_LIBRARY_PATH})
	add_dependencies(${GTEST_LIBRARIES} googletest)
endif()


include_directories(
	"${CMAKE_CURRENT_SOURCE_DIR}/include"
	${GLOG_INCLUDE_DIRS}
	${GTEST_INCLUDE_DIRS}
	${MSGPACK_INCLUDE_DIRS}
	${RIBOSOME_INCLUDE_DIRS}
)

link_directories(
	${GLOG_LIBRARY_DIRS}
	${MSGPACK_LIBRARY_DIRS}
	${RIBOSOME_LIBRARY_DIRS}
)

file(GLOB headers "${CMAKE_CURRENT_SOURCE_DIR}/include/scatter/*.hpp")
install(FILES ${headers} DESTINATION include/scatter)

add_subdirectory(src)
add_subdirectory(test)
add_subdirectory(example)

