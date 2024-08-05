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



/// @brief array insert example
/// @param client 
void ArrayTypeExample(Client& client){
    client.Execute("DROP STREAM IF EXISTS array_example");
    client.Execute("CREATE STREAM IF NOT EXISTS array_example "
                   "(id uint64, arr_int128 array(int128))");

    {
        Block block;

        auto arr_int128 = std::make_shared<ColumnArray>(std::make_shared<ColumnInt128>());

        auto i128 = std::make_shared<ColumnInt128>();

        i128->Append(1);
        i128->Append(2);

        arr_int128->AppendAsColumn(i128);


        block.AppendColumn("arr_int128", arr_int128);

        client.Insert("array_example", block);
    }

    client.Select("SELECT id, arr_int128 FROM array_example", [](const Block& block)
        {
            for (size_t c = 0; c < block.GetRowCount(); ++c) {
                auto col0 = block[0]->As<ColumnUInt64>()->At(c);
                std::cout<< col0 <<std::endl;
                auto col1 = block[1]->As<ColumnArray>()->GetAsColumn(c);
                for (size_t i = 0; i < col1->Size(); ++i) {
                    std::cout << (int)(*col1->As<ColumnInt128>())[i] << " ";
                }
                std::cout << std::endl;
            }
        }
    );

    client.Execute("DROP STREAM IF EXISTS array_example");
}



/// @brief sap insert example
/// @param client 
void SimpleAggregateFunctionTypeExample(Client& client){
    client.Execute("DROP STREAM IF EXISTS saf_example");
    client.Execute("CREATE STREAM IF NOT EXISTS saf_example "
                   "(saf simple_aggregate_function(sum, uint64))");

    {
        constexpr size_t EXPECTED_ROWS = 10;
        client->Execute("INSERT INTO saf_example (saf) VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)");
    }

    client->Select("Select saf FROM saf_example", [&total_rows](const Block & block) {
        auto col = block[0]->As<ColumnUInt64>();
        for (size_t r = 0; r < col->Size(); ++r) {
            std::cout<< r <<std::endl;
        }
    });

    client.Execute("DROP STREAM IF EXISTS saf_example");
}



/// @brief nullable insert example
/// @param client 
void NullableTypeExample(Client& client){
   client.Execute("DROP STREAM IF EXISTS nullable_example");
    client.Execute("CREATE STREAM IF NOT EXISTS nullable_example (id nullable(uint64))");

    {
        Block block;

        {
            auto id = std::make_shared<ColumnUInt64>();
            id->Append(1);
            id->Append(2);

            auto nulls = std::make_shared<ColumnUInt8>();
            nulls->Append(0);
            nulls->Append(1);

            block.AppendColumn("id", std::make_shared<ColumnNullable>(id, nulls));
        }

        client.Insert("nullable_example", block);
    }
    client.Select("SELECT id FROM nullable_example", [](const Block& block)
        {
            for (size_t c = 0; c < block.GetRowCount(); ++c) {
                auto col_id = block[0]->As<ColumnNullable>();
                std::cout<< col_id->Nested()->As<ColumnUInt64>()->At(c) <<std::endl;
            }
        }
    );

    client.Execute("DROP STREAM IF EXISTS nullable_example");
}



/// @brief map insert example
/// @param client 
void MapTypeExample(Client& client){

    client.Execute("DROP STREAM IF EXISTS map_example");
    client.Execute("CREATE STREAM IF NOT EXISTS map_example "
                   "(id uint64, mp map(uint64, string))");

    {
        Block block;

        auto id = std::make_shared<ColumnUInt64>();
        id->Append(1);

        using Mapt = ColumnMapT<ColumnUInt64, ColumnString>;
        auto mp = std::make_shared<Mapt>(std::make_shared<ColumnUInt64>(), std::make_shared<ColumnString>());

        std::map<uint64_t, std::string> row;
        row[1] = "hello";
        row[2] = "world";
        mp->Append(row);

        block.AppendColumn("id", id);
        block.AppendColumn("mp", mp);

        client.Insert("map_example", block);
    }

    client.Select("SELECT id, mp FROM map_example", [](const Block& block)
        {
            for (size_t c = 0; c < block.GetRowCount(); ++c) {
                auto col1 = block[0]->As<ColumnUInt64>()->At(c);
                std::cout<< col1 <<std::endl;

                auto col2 = block[1]->As<ColumnMap>();
                const auto tuples = col2->GetAsColumn(c)->As<ColumnTuple>();
                for(size_t i = 0; i < tuples->Size(); i++){
                    std::cout<< (*tuples)[0]->As<ColumnUInt64>()->At(i)<<std::endl;
                    std::cout<< (*tuples)[1]->As<ColumnString>()->At(i)<<std::endl;
                }

            }
        }
    );

    client.Execute("DROP STREAM IF EXISTS map_example");
}

/// @brief tuple insert example
/// @param client 
void TupleTypeExample(Client& client){

    client.Execute("DROP STREAM IF EXISTS tuple_example");
    client.Execute("CREATE STREAM IF NOT EXISTS tuple_example "
                   "(id uint64, tup tuple(uint64, string))");

    {
        Block block;

        auto id = std::make_shared<ColumnUInt64>();
        id->Append(1);

        auto tupls = std::make_shared<ColumnTuple>(std::vector<ColumnRef>{
            std::make_shared<ColumnUInt64>(),
            std::make_shared<ColumnString>()}
        );

        (*tupls)[0]->As<ColumnUInt64>()->Append(1u);
        (*tupls)[1]->As<ColumnString>()->Append("good");

        block.AppendColumn("id", id);
        block.AppendColumn("tup", tupls);


        client.Insert("tuple_example", block);
    }

    client.Select("SELECT id, tup FROM tuple_example", [](const Block& block)
        {
                for (size_t c = 0; c < block.GetRowCount(); ++c) {
                auto col1 = block[0]->As<ColumnUInt64>()->At(c);
                std::cout<< col1 <<std::endl;

                auto col2 = block[1]->As<ColumnTuple>();
                for(size_t i = 0; i < col2->Size(); i++){
                    std::cout<< (*col2)[0]->As<ColumnUInt64>()->At(i)<<std::endl;
                    std::cout<< (*col2)[1]->As<ColumnString>()->At(i)<<std::endl;
                }

            }
        }
    );

    client.Execute("DROP STREAM IF EXISTS tuple_example");
}

/// @brief nested insert example
/// @param client 
void NestedTypeExample(Client& client){

    client.Execute("DROP STREAM IF EXISTS nested_example");
    client.Execute("CREATE STREAM IF NOT EXISTS nested_example(n nested(id uint64, f float32))");


    {
        Block block;
        auto nested_id = std::make_shared<ColumnArray>(std::make_shared<ColumnUInt64>());
        auto nested_f = std::make_shared<ColumnArray>(std::make_shared<ColumnFloat32>());

        std::vector<uint64_t> id_values{1, 2};
        nested_id->As<ColumnArray>()->AppendAsColumn(std::make_shared<ColumnUInt64>(id_values));

        std::vector<float> f_values{1.1, 2.2};
        nested_f->As<ColumnArray>()->AppendAsColumn(std::make_shared<ColumnFloat32>(f_values));

        block.AppendColumn("n.id", nested_id);
        block.AppendColumn("n.f", nested_f);

        client.Insert("nested_example", block);
    }

    client.Select("SELECT n.id, n.f FROM nested_example", [](const Block& block)
        {
            for (size_t c = 0; c < block.GetRowCount(); ++c) {
                auto colid = block[0]->As<ColumnArray>()->GetAsColumn(c)->As<ColumnUInt64>();
                for(size_t i= 0; i<colid->Size(); ++i){
                    std::cout<< colid->At(i) <<std::endl;
                }

                auto colf = block[1]->As<ColumnArray>()->GetAsColumn(c)->As<ColumnFloat32>();
                for(size_t i= 0; i<colf->Size(); ++i){
                    std::cout<< colf->At(i) <<std::endl;
                }
            }
        }
    );

    client.Execute("DROP STREAM IF EXISTS nested_example");
}
