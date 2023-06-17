#include "byte_stream.hh"
#include <iostream>
#include <stdexcept>

using namespace std;

ByteStream::ByteStream( uint64_t capacity ) : capacity_( capacity ) {}

void Writer::push( string data )
{
  if ( is_closed() ) {
    return;
  }

  size_t to_write_len_ = min( available_capacity(), data.size() );

  buffer_.append( data.substr( 0, to_write_len_ ) );
  num_bytes_written_ += to_write_len_;
}

void Writer::close()
{
  end_write_ = true;
}

void Writer::set_error()
{
  has_error_ = true;
}

bool Writer::is_closed() const
{
  return end_write_;
}

uint64_t Writer::available_capacity() const
{
  return capacity_ - ( num_bytes_written_ - num_bytes_read_ );
}

uint64_t Writer::bytes_pushed() const
{
  return num_bytes_written_;
}

string_view Reader::peek() const
{
  return string_view { buffer_ };
}

bool Reader::is_finished() const
{
  return end_write_ && num_bytes_read_ == num_bytes_written_;
}

bool Reader::has_error() const
{
  return has_error_;
}

void Reader::pop( uint64_t len )
{
  uint64_t to_read_len_ = min( len, buffer_.size() );

  buffer_.erase( buffer_.begin(), buffer_.begin() + to_read_len_ );

  num_bytes_read_ += to_read_len_;
}

uint64_t Reader::bytes_buffered() const
{
  return num_bytes_written_ - num_bytes_read_;
}

uint64_t Reader::bytes_popped() const
{
  return num_bytes_read_;
}
