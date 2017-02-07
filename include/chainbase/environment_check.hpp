#pragma once

namespace chainbase {
   struct environment_check {
      environment_check() {
         memset( &compiler_version, 0, sizeof( compiler_version ) );
         memcpy( &compiler_version, __VERSION__, std::min<size_t>( strlen(__VERSION__), 256 ) );
#ifndef NDEBUG
         debug = true;
#endif
#ifdef __APPLE__
         apple = true;
#endif
#ifdef WIN32
         windows = true;
#endif
      }
      friend bool operator == ( const environment_check& a, const environment_check& b ) {
         return std::make_tuple( a.compiler_version, a.debug, a.apple, a.windows )
            ==  std::make_tuple( b.compiler_version, b.debug, b.apple, b.windows );
      }

      boost::array<char,256>  compiler_version;
      bool                    debug = false;
      bool                    apple = false;
      bool                    windows = false;
   };
}
