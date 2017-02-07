#define BOOST_TEST_MODULE chainbase test

#include <boost/test/unit_test.hpp>
#include <chainbase/chainbase.hpp>

#include <boost/multi_index_container.hpp>
#include <boost/multi_index/ordered_index.hpp>
#include <boost/multi_index/member.hpp>

#include <chainbase/dynamic_database.hpp>

#include <iostream>

using namespace chainbase;
using namespace boost::multi_index;

//BOOST_TEST_SUITE( serialization_tests, clean_database_fixture )

struct book : public chainbase::object<0, book> {

   template<typename Constructor, typename Allocator>
    book(  Constructor&& c, Allocator&& a ) {
       c(*this);
    }

    id_type id;
    int a = 0;
    int b = 1;
};

typedef multi_index_container<
  book,
  indexed_by<
     ordered_unique< member<book,book::id_type,&book::id> >,
     ordered_non_unique< BOOST_MULTI_INDEX_MEMBER(book,int,a) >,
     ordered_non_unique< BOOST_MULTI_INDEX_MEMBER(book,int,b) >
  >,
  chainbase::allocator<book>
> book_index;

CHAINBASE_SET_INDEX_TYPE( book, book_index )


BOOST_AUTO_TEST_CASE( open_and_create ) {
   boost::filesystem::path temp = boost::filesystem::unique_path();
   try {
      std::cerr << temp.native() << " \n";

      chainbase::database db;
      BOOST_CHECK_THROW( db.open( temp ), std::runtime_error ); /// temp does not exist

      db.open( temp, database::read_write, 1024*1024*8 );

      chainbase::database db2; /// open an already created db
      db2.open( temp );
      BOOST_CHECK_THROW( db2.add_index< book_index >(), std::runtime_error ); /// index does not exist in read only database

      db.add_index< book_index >();
      BOOST_CHECK_THROW( db.add_index<book_index>(), std::logic_error ); /// cannot add same index twice


      db2.add_index< book_index >(); /// index should exist now


      BOOST_TEST_MESSAGE( "Creating book" );
      const auto& new_book = db.create<book>( []( book& b ) {
          b.a = 3;
          b.b = 4;
      } );
      const auto& copy_new_book = db2.get( book::id_type(0) );
      BOOST_REQUIRE( &new_book != &copy_new_book ); ///< these are mapped to different address ranges

      BOOST_REQUIRE_EQUAL( new_book.a, copy_new_book.a );
      BOOST_REQUIRE_EQUAL( new_book.b, copy_new_book.b );

      db.modify( new_book, [&]( book& b ) {
          b.a = 5;
          b.b = 6;
      });
      BOOST_REQUIRE_EQUAL( new_book.a, 5 );
      BOOST_REQUIRE_EQUAL( new_book.b, 6 );

      BOOST_REQUIRE_EQUAL( new_book.a, copy_new_book.a );
      BOOST_REQUIRE_EQUAL( new_book.b, copy_new_book.b );

      {
          auto session = db.start_undo_session(true);
          db.modify( new_book, [&]( book& b ) {
              b.a = 7;
              b.b = 8;
          });

         BOOST_REQUIRE_EQUAL( new_book.a, 7 );
         BOOST_REQUIRE_EQUAL( new_book.b, 8 );
      }
      BOOST_REQUIRE_EQUAL( new_book.a, 5 );
      BOOST_REQUIRE_EQUAL( new_book.b, 6 );

      {
          auto session = db.start_undo_session(true);
          const auto& book2 = db.create<book>( [&]( book& b ) {
              b.a = 9;
              b.b = 10;
          });

         BOOST_REQUIRE_EQUAL( new_book.a, 5 );
         BOOST_REQUIRE_EQUAL( new_book.b, 6 );
         BOOST_REQUIRE_EQUAL( book2.a, 9 );
         BOOST_REQUIRE_EQUAL( book2.b, 10 );
      }
      BOOST_CHECK_THROW( db2.get( book::id_type(1) ), std::out_of_range );
      BOOST_REQUIRE_EQUAL( new_book.a, 5 );
      BOOST_REQUIRE_EQUAL( new_book.b, 6 );


      {
          auto session = db.start_undo_session(true);
          db.modify( new_book, [&]( book& b ) {
              b.a = 7;
              b.b = 8;
          });

         BOOST_REQUIRE_EQUAL( new_book.a, 7 );
         BOOST_REQUIRE_EQUAL( new_book.b, 8 );
         session.push();
      }
      BOOST_REQUIRE_EQUAL( new_book.a, 7 );
      BOOST_REQUIRE_EQUAL( new_book.b, 8 );
      db.undo();
      BOOST_REQUIRE_EQUAL( new_book.a, 5 );
      BOOST_REQUIRE_EQUAL( new_book.b, 6 );

      BOOST_REQUIRE_EQUAL( new_book.a, copy_new_book.a );
      BOOST_REQUIRE_EQUAL( new_book.b, copy_new_book.b );
      bfs::remove_all( temp );
   } catch ( ... ) {
      bfs::remove_all( temp );
      throw;
   }
}

BOOST_AUTO_TEST_CASE( dynamic_open_and_create ) {
   boost::filesystem::path temp = boost::filesystem::unique_path();
   try {
      dynamic_multi_database db;
      BOOST_CHECK_THROW( db.open( temp ), std::runtime_error ); /// temp does not exist
      db.open( temp, database::read_write, 1024*1024*2 );

      const auto& testdb = db.create_database( "test" );
      db.modify( testdb, [&]( dynamic_database& ddb ) {
          const auto& baltable = ddb.create_table( "balances" );
          ddb.modify( baltable, [&]( table& t ) {
             const auto& rec = t.index.create( 1, 2, { 'a', 'b', 'c' } );
          });
      });

      const auto& c = db.create( "test", "balances", 4, 3, {'d'} );
      

      std::cout << "c.id: " << c.id <<"\n";

      const auto& t = db.get_database("test").get_table( "balances" );
      const auto& r = t.index.get_by_primary( 1 );
      const auto& c2  = t.index.get_by_primary( 4 );
      const auto& c5  = t.index.get_by_secondary( 3 );
      const auto& c4  = t.index.get_by_id( 2 );
      std::cout << r.primary_key << " " << r.secondary_key << " " << r.value.size() <<"\n";
      std::cout << c.primary_key << " " << c.secondary_key << " " << c.value.size() <<"\n";
      std::cout << c2.primary_key << " " << c2.secondary_key << " " << c2.value.size() <<"\n";

      BOOST_REQUIRE( &c == &c2 );
      BOOST_REQUIRE( &c == &c4 );
      BOOST_REQUIRE( &c == &c5 );

      bfs::remove_all( temp );
   } catch ( ... ) {
      bfs::remove_all( temp );
      throw;
   }
}

// BOOST_AUTO_TEST_SUITE_END()
