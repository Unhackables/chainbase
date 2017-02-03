/**
 *  This file define a more dynamic database that enables creating new database tables
 *  without having to rely upon C++ templates to define the index. It is also designed
 *  to more effeciently scale to a parallel system where different database tables can
 *  be accessed and modified by different threads at the same time.
 *
 *  MultiDatabase - manages multiple named dynamic databases
 *     Named DynamicDatabase - manages multiple named tables
 *        Named Tables - manages multiple records sorted by two keys and an ID
 *           ID, PRIMARY_KEY, SECONDARY_KEY, BLOB_VALUE
 *
 *  When used with a multi-threaded blockchain with many different tables it becomes 
 *  impractical to create undo sessions for all tables when only a few tables actually
 *  have any changes.  Under this design, each dynamic database has a set of tables that
 *  use identical record structures and therefore can share a single UNDO structure.
 *
 *  Each thread in a blockchain would be restricted to accessing a single DynamicDatabase,
 *  when attempting to modify the DynamicDatabase the thread will check to see if the DynamicDatabase
 *  is part of the current global undo state and if it isn't, then add itself. 
 *
 *  Blockchains that use multiple threads to process a block will necessiarally know in advance
 *  which thread is being assigned to which database and therefore knows all databases that will
 *  be accessed by a particular block.  This means all databases which could be modified by a
 *  transaction will be added to the global undo state upfront.
 */
#pragma once
#include <chainbase/chainbase.hpp>

namespace chainbase {

typedef boost::multiprecision::int128_t int128;
typedef vector<char>                    value_type;
typedef shared_vector<char>             shared_value_type;

enum comparison_type {
   integer_compare,
   unsigned_integer_compare,
   string_compare,
   memory_compare
};  

struct record_header {
   uint8_t      primary_compare   = integer_compare;
   uint8_t      secondary_compare = integer_compare;
}

class dynamic_index;

struct record : protected record_header {

   template<typename Constructor, typename Allocator>
   record( Constructor&& c,  allocator<Allocator> a )
   :value( a ) {
      c( *this );
   }

   uint64_t            id = 0;
   int128_t            primary_key;
   int128_t            secondary_key;
   shared_value_type   value;

   friend class dynamic_index; /// allow index access to protected record header
};

struct primary_compare {
   bool operator()( const record& a, const record& b)const {
      return a.primary_key < b.primary_key;
   }
};
struct secondary_compare {
   bool operator()( const record& a, const record& b)const {
      return a.secondary_key < b.secondary_key;
   }
};

struct by_id;
struct by_primary_secondary_id;
struct by_secondary_primary_id;
typedef shared_multi_index_container <
  record,
  indexed_by<
    ordered_unique< tag< by_id >, member< record, uint64_t, &record::id > >,
    ordered_unique< tag< by_primary_secondary_id >, 
      composite_key< record,
         member< record, int128_t, &record::primary_key >
         member< record, int128_t, &record::secondary_key >
         member< record, uint64_t, &record::id >
      >,
      composite_key_compare< primary_compare<int128>, secondary_compare<int128>, std::less<uint64_t> >
    >,
    ordered_unique< tag< by_secondary_primary_id >, 
      composite_key< record,
         member< record, int128_t, &record::primary_key >
         member< record, int128_t, &record::secondary_key >
         member< record, uint64_t, &record::id >
      >,
      composite_key_compare< secondary_compare<int128>, primary_compare<int128>, std::less<uint64_t> >
    >
> record_index;


class dynamic_index {
   public:
      template<typename T>
      dynamic_index( allocator<T> a)
      :_indices( a.get_segment_manager() ) {
      }

      ~dynamic_index();

      const record& create( int128 primary, int128 secondary, const value_type& value ) {
         auto new_id = _next_id;
         auto constructor = [&]( record& rec ) {
            rec.primary_compare   = _primary_compare;
            rec.secondary_compare = _secondary_compare;
            rec.id = id;
            c( rec );
         }
         auto insert_result = _indices.emplace( constructor, _indices.get_allocator() );

         if( !insert_result.second ) {
            BOOST_THROW_EXCEPTION( std::logic_error("could not insert object, most likely a uniqueness constraint was violated") );
         }

         ++_next_id;
         on_create( *insert_result.first );
         return *insert_result.first;
      }

      template<typename Modifier>
      void modify( const record& rec, Modifier&& m ) {
         on_modify( rec );
         auto ok = _indices.modify( _indices.iterator_to( rec ), m );
         if( !ok ) BOOST_THROW_EXCEPTION( std::logic_error( "Could not modify object, most likely a uniqueness constraint was violated" ) );
      }
      
      void modify( const record& rec, int128 primary, int128 secondary, const value_type& v ) {
         modify( rec, [&]( record& r ) {
            r.primary_key = primary;
            r.secondary_key = secondary;
            r.value.resize( v.size() );
            if( v.size() )
               memcpy( r.value.data(), v.data(), v.size() );
         });
      }

      void  remove( const record& rec ) {
         on_remove(rec);
         _indicies.erase( _indicies.iterator_to( rec ) );
      }

      const record* find( uint64_t id );
      const record* find_primary( int128 primary );
      const record* find_secondary( int128 secondary );

   private:
      void on_modify( const record& rec ) {
      }

      void on_create( const record& rec ) {
      }

      void on_remove( const record& rec ) {
      }

      comparison_type _primary_compare   = integer_compare;
      comparison_type _secondary_compare = integer_compare;

      record_index _indicies;

};

struct table {
   template<typename Constructor, typename Allocator>
   table( Constructor&& c, Allocator&& a )
   :name( a.get_segment_manager() ), index( a ) {
      c( *this );
   }

   shared_string name;
   dynamic_index index;
};

struct by_name;
typedef shared_multi_index_container {
   table,
   indexed_by<
      ordered_unique< tag<by_name>, member< table, shared_string, &table::name > >
   >
} table_index;

/**
 *  A dynamic database has an arbitrary number of named tables, each table has 
 *  3 indices (id, primary-secondary-id, and secondary-primary-id) and arbitary data value.
 *  Each primary and secondary are int128 which can be sorted as intergers, strings, or memory in
 *  assending or decending order.  
 *  The ID is assigned and increments from 0 with each new object added
 *
 *  The database as a whole shares a common undo state and requires read/write locks if multiple threads
 *  are going to access the database
 */
class dynamic_database {
   public:
      template<typename T>
      dynamic_database( allocator<T> a )
      :tables(a) {
      }

   private:
      table_index _tables;

};


/**
 * Maintains a collection of dynamic databases allocated in a shared memory file. This is the class
 * responsible for allocating/resizing the database file.  
 */
class multi_dynamic_database {
   public:

      template< typename Lambda >
      auto with_write_lock( Lambda&& callback, uint64_t wait_micro = 1000000 ) -> decltype( (*(Lambda*)nullptr)() );

      template< typename Lambda >
      auto with_read_lock( Lambda&& callback, uint64_t wait_micro = 1000000 ) -> decltype( (*(Lambda*)nullptr)() );

   private:
      /// allocated in shared memory segment
      dynamic_database_index*                                     _indicies;

      unique_ptr<bip::managed_mapped_file>                        _segment;
      unique_ptr<bip::managed_mapped_file>                        _meta;
      read_write_mutex_manager*                                   _rw_manager = nullptr;
      bool                                                        _read_only = false;
      bip::file_lock                                              _flock;
};
} // namespace chainbase
