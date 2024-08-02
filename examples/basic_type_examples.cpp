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


/// @brief int insert example
/// @param client 
void IntTypeExample(Client& client) {

    client.Execute("DROP STREAM IF EXISTS int_example");
    client.Execute("CREATE STREAM IF NOT EXISTS int_example "
    "(id uint64, int8_val int8) ENGINE = Memory");

    Block block;

    auto id = std::make_shared<ColumnUInt64>();
    auto int8_col = std::make_shared<ColumnInt8>();

    id->Append(1);
    id->Append(2);
    id->Append(3);

    int8_col->Append(11);
    int8_col->Append(22);
    int8_col->Append(33);

    block.AppendColumn("id", id);
    block.AppendColumn("int8_val", int8_col);

    client.Insert("int_example", block);

    client.Select("SELECT * FROM int_example", [](const Block& block)
    {
        for (size_t c = 0; c < block.GetRowCount(); ++c) {
            auto col0 = block[0]->As<ColumnUInt64>()->At(c);
            std::cout<< col0 <<std::endl;
            auto col1 = block[1]->As<ColumnInt8>()->At(c);
            std::cout<< col1 <<std::endl;
        }
    }
    );

    client.Execute("DROP STREAM IF EXISTS int_example");

}

/// @brief decimal insert example
/// @param client 
void DecimalTypeExample(Client& client) {

    client.Execute("DROP STREAM IF EXISTS decimal_example");
    client.Execute("CREATE STREAM IF NOT EXISTS decimal_example "
    "(id uint64, decimal_col decimal(9, 4)) ENGINE = Memory");

    Block block;

    auto id = std::make_shared<ColumnUInt64>();
    auto decimal_col = std::make_shared<ColumnDecimal>(9, 4);

    id->Append(1);
    id->Append(2);

    decimal_col->Append(0);
    decimal_col->Append(std::string("12345.6789"));

    block.AppendColumn("id", id);
    block.AppendColumn("decimal_col", decimal_col);

    client.Insert("decimal_example", block);

    client.Select("SELECT * FROM decimal_example", [](const Block& block)
    {
        for (size_t c = 0; c < block.GetRowCount(); ++c) {
            auto col0 = block[0]->As<ColumnUInt64>()->At(c);
            std::cout<< col0 <<std::endl;
            auto col1 = block[1]->As<ColumnDecimal>()->At(c);
            std::cout<< col1 <<std::endl;
        }
    }
    );

    client.Execute("DROP STREAM IF EXISTS decimal_example");

}

/// @brief float insert example
/// @param client 
void FloatTypeExample(Client& client) {
    client.Execute("DROP STREAM IF EXISTS float_example");
    client.Execute("CREATE STREAM IF NOT EXISTS float_example "
    "(id uint64, f32 float32) ENGINE = Memory");

    {
        Block block;

        float max_float32 = std::numeric_limits<float>::max();
        float min_float32 = std::numeric_limits<float>::min();

        auto id = std::make_shared<ColumnUInt64>();
        auto f32 = std::make_shared<ColumnFloat32>();

        id->Append(1);
        id->Append(2);

        f32->Append(min_float32);
        f32->Append(max_float32);

        block.AppendColumn("id", id);
        block.AppendColumn("f32", f32);

        client.Insert("float_example", block);
    }

    client.Select("SELECT * FROM float_example", [](const Block& block)
        {
            for (size_t c = 0; c < block.GetRowCount(); ++c) {
                auto col0 = block[0]->As<ColumnUInt64>()->At(c);
                std::cout<< col0 <<std::endl;
                auto col1 = block[1]->As<ColumnFloat32>()->At(c);
                std::cout<< col1 <<std::endl;
            }
        }
    );

    client.Execute("DROP STREAM IF EXISTS float_example");
}


/// @brief uuid insert example
/// @param client 
void UUIDTypeExample(Client& client) {
    client.Execute("DROP STREAM IF EXISTS uuid_example");
    client.Execute("CREATE STREAM IF NOT EXISTS uuid_example "
    "(id uint64, uuid_col uuid) ENGINE = Memory");

    {
        Block block;

        auto uuid_max = UUID({0xffffffffffffffffllu, 0xffffffffffffffffllu});
        auto uuid_min = UUID({0x0llu, 0x0llu});

        auto id = std::make_shared<ColumnUInt64>();
        auto uuid_col = std::make_shared<ColumnUUID>();

        id->Append(1);
        id->Append(2);

        uuid_col->Append(uuid_min);
        uuid_col->Append(uuid_max);

        block.AppendColumn("id", id);
        block.AppendColumn("uuid_col", uuid_col);

        client.Insert("uuid_example", block);
    }

    client.Select("SELECT * FROM uuid_example", [](const Block& block)
        {
            for (size_t c = 0; c < block.GetRowCount(); ++c) {
                auto col0 = block[0]->As<ColumnUInt64>()->At(c);
                std::cout<< col0 <<std::endl;
                auto col1 = block[1]->As<ColumnUUID>()->At(c);
                std::cout<< col1 <<std::endl;
            }
        }
    );

    client.Execute("DROP STREAM IF EXISTS uuid_example");
}


/// @brief string insert example
/// @param client 
void StringTypeExample(Client& client) {
    client.Execute("DROP STREAM IF EXISTS string_example");
    client.Execute("CREATE STREAM IF NOT EXISTS string_example "
    "(id uint64, s_col string, fs_col fixed_string(10)) ENGINE = Memory");

    {
        Block block;

        auto id = std::make_shared<ColumnUInt64>();
        auto s_col = std::make_shared<ColumnString>();
        auto fs_col = std::make_shared<ColumnFixedString>(10);

        id->Append(1);
        id->Append(2);

        s_col->Append("123");
        s_col->Append("1234567890123");

        fs_col->Append("123");
        fs_col->Append("1234567890123");

        block.AppendColumn("id", id);
        block.AppendColumn("s_col", s_col);
        block.AppendColumn("fs_col", fs_col);

        client.Insert("string_example", block);
    }

    client.Select("SELECT * FROM string_example", [](const Block& block)
        {
            for (size_t c = 0; c < block.GetRowCount(); ++c) {
                auto col0 = block[0]->As<ColumnUInt64>()->At(c);
                std::cout<< col0 <<std::endl;
                auto col1 = block[1]->As<ColumnString>()->At(c);
                std::cout<< col1 <<std::endl;
                auto col2 = block[2]->As<ColumnFixedString>()->At(c);
                std::cout<< col2 <<std::endl;
            }
        }
    );

    client.Execute("DROP STREAM IF EXISTS string_example");
}


/// @brief enum insert example
/// @param client 
void EnumTypeExample(Client& client) {

    client.Execute("DROP STREAM IF EXISTS enum_example");
    client.Execute("CREATE STREAM IF NOT EXISTS enum_example (id uint64, e1 enum8('Min' = -128, 'Max' = 127), e2 enum16('Mn' = -32768, 'Mx' = 32767)) ENGINE = Memory");

    {
        Block block;

        auto id = std::make_shared<ColumnUInt64>();
        id->Append(1);
        id->Append(2);

        auto e1 = std::make_shared<ColumnEnum8>(Type::CreateEnum8({{"Min", -128}, {"Max", 127}}));
        e1->Append(-128);
        e1->Append("Max");

        auto e2 = std::make_shared<ColumnEnum16>(Type::CreateEnum16({{"Mn", -32768}, {"Mx", 32767}}));
        e2->Append("Mn");
        e2->Append("Mx");

        block.AppendColumn("id", id);
        block.AppendColumn("e1", e1);
        block.AppendColumn("e2", e2);

        client.Insert("enum_example", block);
    }

    client.Select("SELECT id, e1, e2 FROM enum_example", [](const Block& block)
        {
            for (size_t c = 0; c < block.GetRowCount(); ++c) {
                auto col0 = block[0]->As<ColumnUInt64>()->At(c);
                std::cout<< col0 <<std::endl;
                auto col1 = block[1]->As<ColumnEnum8>()->At(c);
                std::cout<< col1 <<std::endl;
                auto col2 = block[2]->As<ColumnEnum16>()->At(c);
                std::cout<< col2 <<std::endl;
            }
        }
    );

    client.Execute("DROP STREAM IF EXISTS enum_example");
}

/// @brief ip insert example
/// @param client 
void IPTypeExample(Client & client) {
    client.Execute("DROP STREAM IF EXISTS ip_example");
    client.Execute("CREATE STREAM IF NOT EXISTS ip_example (id uint64, v4 ipv4, v6 ipv6) ENGINE = Memory");

    {
        Block block;

        auto id = std::make_shared<ColumnUInt64>();
        id->Append(1);
        id->Append(2);
        id->Append(3);

        auto v4 = std::make_shared<ColumnIPv4>();
        v4->Append("127.0.0.1");
        v4->Append(3585395774);
        v4->Append(0);

        auto v6 = std::make_shared<ColumnIPv6>();
        v6->Append("::1");
        v6->Append("aa::ff");
        v6->Append("fe80::86ba:ef31:f2d8:7e8b");

        block.AppendColumn("id", id);
        block.AppendColumn("v4", v4);
        block.AppendColumn("v6", v6);

        client.Insert("ip_example", block);
    }

    client.Select("SELECT id, v4, v6 FROM ip_example", [&](const Block& block)
        {
            for (size_t c = 0; c < block.GetRowCount(); ++c) {
                auto col0 = block[0]->As<ColumnUInt64>()->At(c);
                std::cout<< col0 <<std::endl;
                auto col1 = block[1]->As<ColumnIPv4>()->At(c);
                std::cout<< col1 <<std::endl;
                auto col2 = block[2]->As<ColumnIPv6>()->At(c);
                std::cout<< col2 <<std::endl;
            }
        }
    );

    client.Execute("DROP STREAM IF EXISTS ip_example");
}


/// @brief date & datetime insert example
/// @param client 
void DateTimeTypeExample(Client& client){

    client.Execute("DROP STREAM IF EXISTS datetime_example");
    client.Execute("CREATE STREAM IF NOT EXISTS datetime_example (id uint64, d date, dt datetime,) ENGINE = Memory");

    {
        Block block;
        
        auto id = std::make_shared<ColumnUInt64>();
        auto d = std::make_shared<ColumnDate>();
        auto dt = std::make_shared<ColumnDateTime>();

        id->Append(1);
        id->Append(2);

        d->Append(0);
        d->Append(std::time(nullptr));

        dt->Append(0);
        dt->Append(std::time(nullptr));

        block.AppendColumn("id", id);
        block.AppendColumn("d", d);
        block.AppendColumn("dt", dt);

        client.Insert("datetime_example", block);
    }

    client.Select("SELECT * FROM datetime_example", [&](const Block& block)
        {
            for (size_t c = 0; c < block.GetRowCount(); ++c) {
                auto col0 = block[0]->As<ColumnUInt64>()->At(c);
                std::cout<< col0 <<std::endl;
                auto col1 = block[1]->As<ColumnDate>()->At(c);
                std::cout<< col1 <<std::endl;
                auto col2 = block[2]->As<ColumnDateTime>()->At(c);
                std::cout<< col2 <<std::endl;
            }
        }
    );

    client.Execute("DROP STREAM IF EXISTS datetime_example");
}
