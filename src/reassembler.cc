#include "reassembler.hh"

using namespace std;

bool Reassembler::is_closed() const
{
  return closed_ && bytes_pending() == 0;
}

void Reassembler::insert( uint64_t first_index, string data, bool is_last_substring, Writer& output )
{
  if ( is_last_substring ) {
    closed_ = true;
  }

  if ( data.empty() || output.available_capacity() == 0
       || first_index >= first_unassembled_idx_ + output.available_capacity() // First index is out of bound.
       || first_index + data.length() <= first_unassembled_idx_               // Data was tranferred.
  ) {
    if ( is_closed() ) {
      output.close();
    }
    return;
  }

  // 1) FRONT-LINE REMOVAL ACCORDING TO STREAM CAP.
  uint64_t stream_cap = output.available_capacity();
  uint64_t actual_first_index = first_index;
  string actual_data;
  if ( first_index < first_unassembled_idx_ ) { // Substring data given is overlapped with the transferred data.
    actual_first_index = first_unassembled_idx_;
    uint64_t overlapped_len_ = first_unassembled_idx_ - first_index;
    actual_data = std::move( data.substr( overlapped_len_, min( data.length() - overlapped_len_, stream_cap ) ) );
  } else {                                                                       // No overlap
    actual_data = std::move( data.substr( 0, min( data.size(), stream_cap ) ) ); // Unfit stream cap
    if ( first_index + actual_data.length()
         > first_unassembled_idx_ + stream_cap ) { // 'first_index' is greater than 'first_unassembled_idx_'.
      actual_data = std::move( data.substr( 0, first_unassembled_idx_ + stream_cap - first_index ) );
    }
  }

  // 2) MERGE OVERLAPPED PARTS AFTER 'actual_first_index'.
  auto rear_iter = unassembled_substrings_.lower_bound( actual_first_index );
  while ( rear_iter != unassembled_substrings_.end() ) {
    uint64_t cur_idx = ( *rear_iter ).first;
    string cur_data = ( *rear_iter ).second;

    if ( actual_first_index + actual_data.length()
         <= cur_idx ) { // Last byte to be inserted is not greater than cur_idx_, stop here.
      break;
    }

    uint64_t next_lowerbound
      = cur_idx + cur_data.length(); // Calculate lower bound early as the data might be deleted.
    uint64_t overlapped_len = 0;
    if ( actual_first_index + actual_data.length() < cur_idx + cur_data.length() ) {
      overlapped_len = actual_first_index + actual_data.length() - cur_idx;
    } else {
      overlapped_len = cur_data.length();
    }

    if ( overlapped_len == cur_data.length() ) {
      unassembled_bytes_ -= cur_data.length();
      unassembled_substrings_.erase( cur_idx );
    } else {
      actual_data.erase( actual_data.length() - overlapped_len ); // Erase only the overlapped part.
    }
    rear_iter = unassembled_substrings_.lower_bound( next_lowerbound );
  }

  // 3) CLEAR THE PORTION OVERLAPPING WITH THE ONE SUBSTRING STARTING BEFORE 'actual_first_idx'.
  if ( first_index > first_unassembled_idx_ ) {
    auto preceding_iter = unassembled_substrings_.upper_bound( actual_first_index );
    if ( preceding_iter != unassembled_substrings_.begin() ) {
      preceding_iter--; // Place at the correct position.
      uint64_t preceding_idx = ( *preceding_iter ).first;
      string preceding_data = ( *preceding_iter ).second;

      if ( preceding_idx + preceding_data.length() > first_index ) {
        uint64_t overlapped_len = 0;
        if ( preceding_idx + preceding_data.length() <= first_index + actual_data.length() ) {
          overlapped_len = preceding_idx + preceding_data.length() - first_index;
        } else {
          overlapped_len = actual_data.length();
        }

        if ( overlapped_len == preceding_data.length() ) {
          unassembled_bytes_ -= preceding_data.length();
          unassembled_substrings_.erase( preceding_idx );
        } else {
          actual_data.erase( actual_data.begin(), actual_data.begin() + overlapped_len );
          actual_first_index += overlapped_len;
        }
      }
    }
  }

  // 4) IF NOT EMPTY, PUT THE MODIFIED SUBSTRING TO THE INTERNAL STORAGE.
  if ( actual_data.length() > 0 ) {
    unassembled_bytes_ += actual_data.length();
    unassembled_substrings_.insert( { actual_first_index, std::move( actual_data ) } );
  }

  // 5) IF POSSIBLE, PUSH ANY SUBSTRINGS TO THE STREAM.
  for ( auto iter = unassembled_substrings_.begin(); iter != unassembled_substrings_.end(); ) {
    uint64_t cur_idx = ( *iter ).first;
    string cur_data = ( *iter ).second;

    if ( cur_idx == first_unassembled_idx_ ) {
      uint64_t prev_bytes_pushed = output.bytes_pushed();
      output.push( cur_data );
      uint64_t cur_bytes_pushed = output.bytes_pushed();

      if ( cur_bytes_pushed != prev_bytes_pushed + cur_data.length() ) {
        // Cannot push all data (accumulative bytes exceeds stream cap).
        uint64_t pushed_len = cur_bytes_pushed - prev_bytes_pushed;
        first_unassembled_idx_ += pushed_len;
        unassembled_bytes_ -= pushed_len;
        unassembled_substrings_.insert( { first_unassembled_idx_, std::move( cur_data.substr( pushed_len ) ) } );
        unassembled_substrings_.erase( cur_idx );
        break;
      } else {
        first_unassembled_idx_ += cur_data.length();
        unassembled_bytes_ -= cur_data.length();
        unassembled_substrings_.erase( cur_idx );
        iter = unassembled_substrings_.find( first_unassembled_idx_ );
      }
    } else {
      break;
    }
  }

  if ( is_closed() ) {
    output.close();
  }
}

uint64_t Reassembler::bytes_pending() const
{
  return unassembled_bytes_;
}