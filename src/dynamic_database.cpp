#include <chainbase/dynamic_database.hpp>
#include <chainbase/environment_check.hpp>

namespace chainbase {

dynamic_index::~dynamic_index() {
}

dynamic_multi_database::dynamic_multi_database()
{
}

dynamic_multi_database::~dynamic_multi_database() {
   close();
}

void dynamic_multi_database::open( const bfs::path& dir, uint32_t flags, uint64_t shared_file_size ) {
   bool write = flags & read_write;

   if( !bfs::exists( dir ) ) {
      if( !write ) BOOST_THROW_EXCEPTION( std::runtime_error( "database file not found at " + dir.native() ) );
   }

   bfs::create_directories( dir );
   if( _data_dir != dir ) close();

   _data_dir = dir;
   auto abs_path = bfs::absolute( dir / "shared_memory.bin" );

   if( bfs::exists( abs_path ) )
   {
      if( write )
      {
         auto existing_file_size = bfs::file_size( abs_path );
         if( shared_file_size > existing_file_size )
         {
            if( !bip::managed_mapped_file::grow( abs_path.generic_string().c_str(), shared_file_size - existing_file_size ) )
               BOOST_THROW_EXCEPTION( std::runtime_error( "could not grow database file to requested size." ) );
         }

         _segment.reset( new bip::managed_mapped_file( bip::open_only,
                                                       abs_path.generic_string().c_str()
                                                       ) );
      } else {
         _segment.reset( new bip::managed_mapped_file( bip::open_read_only,
                                                       abs_path.generic_string().c_str()
                                                       ) );
         _read_only = true;
      }

      auto env = _segment->find< environment_check >( "environment" );
      if( !env.first || !( *env.first == environment_check()) ) {
         BOOST_THROW_EXCEPTION( std::runtime_error( "database created by a different compiler, build, or operating system" ) );
      }
   } else {
      _segment.reset( new bip::managed_mapped_file( bip::create_only,
                                                    abs_path.generic_string().c_str(), shared_file_size
                                                    ) );
      _segment->find_or_construct< environment_check >( "environment" )();
   }


   abs_path = bfs::absolute( dir / "shared_memory.meta" );

   if( bfs::exists( abs_path ) )
   {
      _meta.reset( new bip::managed_mapped_file( bip::open_only, abs_path.generic_string().c_str()
                                                 ) );

      _rw_manager = _meta->find< read_write_mutex_manager >( "rw_manager" ).first;
      if( !_rw_manager )
         BOOST_THROW_EXCEPTION( std::runtime_error( "could not find read write lock manager" ) );
   }
   else
   {
      _meta.reset( new bip::managed_mapped_file( bip::create_only,
                                                 abs_path.generic_string().c_str(), sizeof( read_write_mutex_manager ) * 2
                                                 ) );

      _rw_manager = _meta->find_or_construct< read_write_mutex_manager >( "rw_manager" )();
   }

   if( write )
   {
      _flock = bip::file_lock( abs_path.generic_string().c_str() );
      if( !_flock.try_lock() )
         BOOST_THROW_EXCEPTION( std::runtime_error( "could not gain write access to the shared memory file" ) );
   }

    if( !_read_only ) {
       _indices = _segment->find_or_construct< dynamic_database_index >( "dynamic_database_index" )(  _segment->get_segment_manager()  );
    } else {
       _indices = _segment->find< dynamic_database_index >( "dynamic_database_index" ).first;
       if( !_indices ) 
          BOOST_THROW_EXCEPTION( std::runtime_error( "unable to find dynamic_database_index in read only database" ) );
    }

} // open

void dynamic_multi_database::close() {
   _segment.reset();
   _meta.reset();
   _data_dir = bfs::path();
   _indices = nullptr;
} // close

const table& dynamic_database::create_table( const string& name ) {
   _tables.emplace( [&]( table& db ) {
      db.name.append( name.begin(), name.end() );
      }, _tables.get_allocator() );
   return get_table( name );
}

const table& dynamic_database::get_table( const string& name )const {
   auto db = find_table( name );
   if( !db )
      BOOST_THROW_EXCEPTION( std::runtime_error( "unable to find database with name: " + name ) );
   return *db;
}

const table* dynamic_database::find_table( const string& name )const {
   auto itr = _tables.find( name );
   if( itr == _tables.end() ) 
      return nullptr;
   return &*itr;
}


const dynamic_database& dynamic_multi_database::create_database( const string& name ) {
   assert( _indices != nullptr );
   _indices->emplace( [&]( dynamic_database& db ) {
      db._name.append( name.begin(), name.end() );
      }, _indices->get_allocator() );
   return get_database( name );
}

const dynamic_database& dynamic_multi_database::get_database( const string& name )const {
   assert( _indices != nullptr );
   auto db = find_database( name );
   if( !db )
      BOOST_THROW_EXCEPTION( std::runtime_error( "unable to find database with name: " + name ) );
   return *db;
}

const dynamic_database* dynamic_multi_database::find_database( const string& name )const {
   assert( _indices != nullptr );
   auto itr = _indices->find( name );
   if( itr == _indices->end() ) 
      return nullptr;
   return &*itr;
}

void dynamic_multi_database::remove_database( const string& name ) {
   assert( _indices != nullptr );
   const auto& db = get_database( name );
   _indices->erase( _indices->iterator_to(db) );
}


const record& dynamic_index::get_by_id( uint32_t id )const {
  auto r = find_by_id( id );
  if( !r ) {
     BOOST_THROW_EXCEPTION( std::runtime_error( "unable to find record by id key" ) );
  }
  return *r;
}

const record& dynamic_index::get_by_primary( int128 primary )const {
  auto r = find_by_primary( primary );
  if( !r ) {
     BOOST_THROW_EXCEPTION( std::runtime_error( "unable to find record by primary key" ) );
  }
  return *r;
}

const record& dynamic_index::get_by_secondary( int128 secondary )const {
  auto r = find_by_secondary( secondary );
  if( !r ) {
     BOOST_THROW_EXCEPTION( std::runtime_error( "unable to find record by secondary key" ) );
  }
  return *r;
}

const record* dynamic_index::find_by_id( uint32_t id )const {
   const auto& idx = _indices.get<by_id>();
   auto itr = idx.find( id );
   if( itr != idx.end() )
      return &*itr;
   return nullptr;
}

const record* dynamic_index::find_by_primary( int128 primary )const {
   const auto& idx = _indices.get<by_primary_secondary_id>();
   auto itr = idx.find( primary );
   if( itr != idx.end() )
      return &*itr;
   return nullptr;
}

const record* dynamic_index::find_by_secondary( int128 secondary )const {
   const auto& idx = _indices.get<by_secondary_primary_id>();
   auto itr = idx.find( secondary );
   if( itr != idx.end() )
      return &*itr;
   return nullptr;
}


}
