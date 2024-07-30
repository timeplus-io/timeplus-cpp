#pragma once

#include <timeplus/client.h>
#include <timeplus/error_codes.h>
#include <timeplus/types/type_parser.h>
#include <timeplus/base/socket.h>

#include <ut/utils.h>

#include <stdexcept>
#include <iostream>
#include <cmath>
#include <thread>
#include <iostream>
#if defined(_MSC_VER)
#   pragma warning(disable : 4996)
#endif
using namespace std;

using namespace timeplus;

constexpr Int256 max_val_256 = std::numeric_limits<Int256>::max();
constexpr Int256 min_val_256 = std::numeric_limits<Int256>::min();
constexpr UInt256 max_val_u256 = std::numeric_limits<UInt256>::max();


constexpr Int128 max_val_128 = std::numeric_limits<Int128>::max();
constexpr Int128 min_val_128 = std::numeric_limits<Int128>::min();
constexpr UInt128 max_val_u128 = std::numeric_limits<UInt128>::max();


void testIntType(Client& client);
void testDecimalType(Client& client);
void testArrayType(Client& client);
void testDateTimeType(Client& client);
void testFloatType(Client& client);
void testUUIDType(Client& client);
void testStringType(Client& client);
void testEnumType(Client& client);
void testIPType(Client& client);
void testMultitesArrayType(Client& client);
void testNullabletype(Client& client);