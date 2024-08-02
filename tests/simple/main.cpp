#include "TestFunctions.h"


static void RunTests(Client& client) {
    testIntType(client);
    testDecimalType(client);
    testArrayType(client);
    testDateTimeType(client);
    testFloatType(client);
    testUUIDType(client);
    testStringType(client);
    testEnumType(client);
    testIPType(client);
    testMultitesArrayType(client);
    testNullabletype(client);
    testLowCardinalityStringType(client);
    testMapType(client);
    testTupleType(client);
    testNestedType(client);
}

int main()
{
    /// Initialize client connection.
    try {
        const auto localHostEndpoint = ClientOptions()
                .SetHost("localhost")
                .SetPort(8463);

        {
            Client client(ClientOptions(localHostEndpoint)
                    .SetPingBeforeQuery(true));
            RunTests(client);
        }

    } catch (const std::exception& e) {
        std::cerr << "exception : " << e.what() << std::endl;
    }


    return 0;
}
