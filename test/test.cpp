#define BOOST_TEST_MODULE chainbase test

#include <boost/test/unit_test.hpp>
#include <chainbase/chainbase.hpp>

#include <boost/multi_index_container.hpp>
#include <boost/multi_index/ordered_index.hpp>
#include <boost/multi_index/member.hpp>

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

struct temp_directory
{
   temp_directory() { path = bfs::temp_directory_path() / bfs::unique_path(); }
   ~temp_directory() { bfs::remove_all( path ); }

   boost::filesystem::path path;
};

BOOST_AUTO_TEST_CASE( open_and_create )
{
   temp_directory temp;

   std::cerr << temp.path.native() << " \n";

   chainbase::database db;
   BOOST_CHECK_THROW( db.open( temp.path ), std::runtime_error ); /// temp does not exist

   db.open( temp.path, database::read_write, 1024*1024*8 );

   chainbase::database db2; /// open an already created db
   db2.open( temp.path );
   BOOST_CHECK_THROW( db2.add_index< book_index >(), std::runtime_error ); /// index does not exist in read only database

   db.add_index< book_index >();
   BOOST_CHECK_THROW( db.add_index<book_index>(), std::logic_error ); /// cannot add same index twice


   db2.add_index< book_index >(); /// index should exist now


   BOOST_TEST_MESSAGE( "Creating book" );
   const auto& new_book = db.create<book>( []( book& b )
   {
      b.a = 3;
      b.b = 4;
   } );
   const auto& copy_new_book = db2.get( book::id_type(0) );
   BOOST_REQUIRE( &new_book != &copy_new_book ); ///< these are mapped to different address ranges

   BOOST_REQUIRE_EQUAL( new_book.a, copy_new_book.a );
   BOOST_REQUIRE_EQUAL( new_book.b, copy_new_book.b );

   db.modify( new_book, [&]( book& b )
   {
      b.a = 5;
      b.b = 6;
   });
   BOOST_REQUIRE_EQUAL( new_book.a, 5 );
   BOOST_REQUIRE_EQUAL( new_book.b, 6 );

   BOOST_REQUIRE_EQUAL( new_book.a, copy_new_book.a );
   BOOST_REQUIRE_EQUAL( new_book.b, copy_new_book.b );

   {
      auto session = db.start_undo_session(true);
      db.modify( new_book, [&]( book& b )
      {
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
      const auto& book2 = db.create<book>( [&]( book& b )
      {
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
      db.modify( new_book, [&]( book& b )
      {
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

   db.wipe( temp.path );
   BOOST_REQUIRE( !bfs::exists( temp.path / "shared_memory.bin") );
}


void createDatabaseOne( boost::filesystem::path& temp )
{
   chainbase::database db;
   try
   {
      db.open( temp, database::read_write, 1024*1024*8 );
      for(;;)
      {
         boost::this_thread::sleep( boost::posix_time::milliseconds( 100 ) );
      }
   }
   catch( boost::thread_interrupted& )
   {
      db.close();
      return;
   }
}


BOOST_AUTO_TEST_CASE( lock_test ) {
   temp_directory temp;

   std::cerr << temp.path.native() << " \n";
   BOOST_TEST_MESSAGE( "Creating Database in thread 1");
   boost::thread t(&createDatabaseOne, temp.path);
   BOOST_TEST_MESSAGE( "Opening Database in thread 2");
   chainbase::database db, db2;
   BOOST_CHECK_THROW(db.open( temp.path, database::read_write ), boost::exception );
   t.interrupt();
   t.join();
   db.open( temp.path, database::read_write );
   db2.open( temp.path, database::read_write );
   db.with_write_lock( [&](){} );
   BOOST_REQUIRE_EQUAL(db.get_current_lock(), 0 );

   for(int i = 0; i<CHAINBASE_NUM_RW_LOCKS; i++)
   {
      db.with_write_lock( [&](){
            BOOST_REQUIRE_EQUAL(db.get_current_lock(), i % CHAINBASE_NUM_RW_LOCKS);
            BOOST_REQUIRE_EQUAL(db2.get_current_lock(), i % CHAINBASE_NUM_RW_LOCKS);
            db2.with_write_lock([](){}, 10000);
      });
   }
   for(int i = 0; i<CHAINBASE_NUM_RW_LOCKS; i++)
   {
      db.with_read_lock( [&](){
            BOOST_REQUIRE_EQUAL(db.get_current_lock(), i % CHAINBASE_NUM_RW_LOCKS);
            BOOST_REQUIRE_EQUAL(db2.get_current_lock(), i % CHAINBASE_NUM_RW_LOCKS);
            db2.with_write_lock([](){}, 10000);
      });
   }
   db.close();
   db2.close();
}

#include <fstream>
BOOST_AUTO_TEST_CASE( schema_test )
{
   temp_directory temp;

   std::cerr << temp.path.native() << " \n";
   auto abs_path = bfs::absolute( temp.path / "shared_memory.bin" );

   BOOST_TEST_MESSAGE( "Creating Database");
   chainbase::database db;
   db.open( temp.path, database::read_write, 1024*1024*8 );
   db.commit(1);
   db.close();
   BOOST_TEST_MESSAGE( "Corrupting Database");
   std::fstream abs_file(abs_path.generic_string().c_str());
   if(!abs_file.is_open())
      BOOST_THROW_EXCEPTION(std::runtime_error("Couldn't open 'shared_memory.bin'"));

   abs_file.seekp(270, std::ios::beg);
   abs_file.write("CORRUPTCORRUPTCORRUPTCORRUPT", 28);
   abs_file.close();
   BOOST_CHECK_THROW( db.open( temp.path, database::read_only), std::runtime_error);
}
// BOOST_AUTO_TEST_SUITE_END()
