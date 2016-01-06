// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: pink.proto

#ifndef PROTOBUF_pink_2eproto__INCLUDED
#define PROTOBUF_pink_2eproto__INCLUDED

#include <string>

#include <google/protobuf/stubs/common.h>

#if GOOGLE_PROTOBUF_VERSION < 2003000
#error This file was generated by a newer version of protoc which is
#error incompatible with your Protocol Buffer headers.  Please update
#error your headers.
#endif
#if 2003000 < GOOGLE_PROTOBUF_MIN_PROTOC_VERSION
#error This file was generated by an older version of protoc which is
#error incompatible with your Protocol Buffer headers.  Please
#error regenerate this file with a newer version of protoc.
#endif

#include <google/protobuf/generated_message_util.h>
#include <google/protobuf/repeated_field.h>
#include <google/protobuf/extension_set.h>
#include <google/protobuf/generated_message_reflection.h>
// @@protoc_insertion_point(includes)

namespace pink {

// Internal implementation detail -- do not call these.
void  protobuf_AddDesc_pink_2eproto();
void protobuf_AssignDesc_pink_2eproto();
void protobuf_ShutdownFile_pink_2eproto();

class Ping;
class PingRes;

// ===================================================================

class Ping : public ::google::protobuf::Message {
 public:
  Ping();
  virtual ~Ping();
  
  Ping(const Ping& from);
  
  inline Ping& operator=(const Ping& from) {
    CopyFrom(from);
    return *this;
  }
  
  inline const ::google::protobuf::UnknownFieldSet& unknown_fields() const {
    return _unknown_fields_;
  }
  
  inline ::google::protobuf::UnknownFieldSet* mutable_unknown_fields() {
    return &_unknown_fields_;
  }
  
  static const ::google::protobuf::Descriptor* descriptor();
  static const Ping& default_instance();
  
  void Swap(Ping* other);
  
  // implements Message ----------------------------------------------
  
  Ping* New() const;
  void CopyFrom(const ::google::protobuf::Message& from);
  void MergeFrom(const ::google::protobuf::Message& from);
  void CopyFrom(const Ping& from);
  void MergeFrom(const Ping& from);
  void Clear();
  bool IsInitialized() const;
  
  int ByteSize() const;
  bool MergePartialFromCodedStream(
      ::google::protobuf::io::CodedInputStream* input);
  void SerializeWithCachedSizes(
      ::google::protobuf::io::CodedOutputStream* output) const;
  ::google::protobuf::uint8* SerializeWithCachedSizesToArray(::google::protobuf::uint8* output) const;
  int GetCachedSize() const { return _cached_size_; }
  private:
  void SharedCtor();
  void SharedDtor();
  void SetCachedSize(int size) const;
  public:
  
  ::google::protobuf::Metadata GetMetadata() const;
  
  // nested types ----------------------------------------------------
  
  // accessors -------------------------------------------------------
  
  // required string address = 2;
  inline bool has_address() const;
  inline void clear_address();
  static const int kAddressFieldNumber = 2;
  inline const ::std::string& address() const;
  inline void set_address(const ::std::string& value);
  inline void set_address(const char* value);
  inline void set_address(const char* value, size_t size);
  inline ::std::string* mutable_address();
  
  // required int32 port = 3;
  inline bool has_port() const;
  inline void clear_port();
  static const int kPortFieldNumber = 3;
  inline ::google::protobuf::int32 port() const;
  inline void set_port(::google::protobuf::int32 value);
  
  // @@protoc_insertion_point(class_scope:pink.Ping)
 private:
  ::google::protobuf::UnknownFieldSet _unknown_fields_;
  mutable int _cached_size_;
  
  ::std::string* address_;
  static const ::std::string _default_address_;
  ::google::protobuf::int32 port_;
  friend void  protobuf_AddDesc_pink_2eproto();
  friend void protobuf_AssignDesc_pink_2eproto();
  friend void protobuf_ShutdownFile_pink_2eproto();
  
  ::google::protobuf::uint32 _has_bits_[(2 + 31) / 32];
  
  // WHY DOES & HAVE LOWER PRECEDENCE THAN != !?
  inline bool _has_bit(int index) const {
    return (_has_bits_[index / 32] & (1u << (index % 32))) != 0;
  }
  inline void _set_bit(int index) {
    _has_bits_[index / 32] |= (1u << (index % 32));
  }
  inline void _clear_bit(int index) {
    _has_bits_[index / 32] &= ~(1u << (index % 32));
  }
  
  void InitAsDefaultInstance();
  static Ping* default_instance_;
};
// -------------------------------------------------------------------

class PingRes : public ::google::protobuf::Message {
 public:
  PingRes();
  virtual ~PingRes();
  
  PingRes(const PingRes& from);
  
  inline PingRes& operator=(const PingRes& from) {
    CopyFrom(from);
    return *this;
  }
  
  inline const ::google::protobuf::UnknownFieldSet& unknown_fields() const {
    return _unknown_fields_;
  }
  
  inline ::google::protobuf::UnknownFieldSet* mutable_unknown_fields() {
    return &_unknown_fields_;
  }
  
  static const ::google::protobuf::Descriptor* descriptor();
  static const PingRes& default_instance();
  
  void Swap(PingRes* other);
  
  // implements Message ----------------------------------------------
  
  PingRes* New() const;
  void CopyFrom(const ::google::protobuf::Message& from);
  void MergeFrom(const ::google::protobuf::Message& from);
  void CopyFrom(const PingRes& from);
  void MergeFrom(const PingRes& from);
  void Clear();
  bool IsInitialized() const;
  
  int ByteSize() const;
  bool MergePartialFromCodedStream(
      ::google::protobuf::io::CodedInputStream* input);
  void SerializeWithCachedSizes(
      ::google::protobuf::io::CodedOutputStream* output) const;
  ::google::protobuf::uint8* SerializeWithCachedSizesToArray(::google::protobuf::uint8* output) const;
  int GetCachedSize() const { return _cached_size_; }
  private:
  void SharedCtor();
  void SharedDtor();
  void SetCachedSize(int size) const;
  public:
  
  ::google::protobuf::Metadata GetMetadata() const;
  
  // nested types ----------------------------------------------------
  
  // accessors -------------------------------------------------------
  
  // required int32 res = 1;
  inline bool has_res() const;
  inline void clear_res();
  static const int kResFieldNumber = 1;
  inline ::google::protobuf::int32 res() const;
  inline void set_res(::google::protobuf::int32 value);
  
  // required bytes mess = 2;
  inline bool has_mess() const;
  inline void clear_mess();
  static const int kMessFieldNumber = 2;
  inline const ::std::string& mess() const;
  inline void set_mess(const ::std::string& value);
  inline void set_mess(const char* value);
  inline void set_mess(const void* value, size_t size);
  inline ::std::string* mutable_mess();
  
  // @@protoc_insertion_point(class_scope:pink.PingRes)
 private:
  ::google::protobuf::UnknownFieldSet _unknown_fields_;
  mutable int _cached_size_;
  
  ::google::protobuf::int32 res_;
  ::std::string* mess_;
  static const ::std::string _default_mess_;
  friend void  protobuf_AddDesc_pink_2eproto();
  friend void protobuf_AssignDesc_pink_2eproto();
  friend void protobuf_ShutdownFile_pink_2eproto();
  
  ::google::protobuf::uint32 _has_bits_[(2 + 31) / 32];
  
  // WHY DOES & HAVE LOWER PRECEDENCE THAN != !?
  inline bool _has_bit(int index) const {
    return (_has_bits_[index / 32] & (1u << (index % 32))) != 0;
  }
  inline void _set_bit(int index) {
    _has_bits_[index / 32] |= (1u << (index % 32));
  }
  inline void _clear_bit(int index) {
    _has_bits_[index / 32] &= ~(1u << (index % 32));
  }
  
  void InitAsDefaultInstance();
  static PingRes* default_instance_;
};
// ===================================================================


// ===================================================================

// Ping

// required string address = 2;
inline bool Ping::has_address() const {
  return _has_bit(0);
}
inline void Ping::clear_address() {
  if (address_ != &_default_address_) {
    address_->clear();
  }
  _clear_bit(0);
}
inline const ::std::string& Ping::address() const {
  return *address_;
}
inline void Ping::set_address(const ::std::string& value) {
  _set_bit(0);
  if (address_ == &_default_address_) {
    address_ = new ::std::string;
  }
  address_->assign(value);
}
inline void Ping::set_address(const char* value) {
  _set_bit(0);
  if (address_ == &_default_address_) {
    address_ = new ::std::string;
  }
  address_->assign(value);
}
inline void Ping::set_address(const char* value, size_t size) {
  _set_bit(0);
  if (address_ == &_default_address_) {
    address_ = new ::std::string;
  }
  address_->assign(reinterpret_cast<const char*>(value), size);
}
inline ::std::string* Ping::mutable_address() {
  _set_bit(0);
  if (address_ == &_default_address_) {
    address_ = new ::std::string;
  }
  return address_;
}

// required int32 port = 3;
inline bool Ping::has_port() const {
  return _has_bit(1);
}
inline void Ping::clear_port() {
  port_ = 0;
  _clear_bit(1);
}
inline ::google::protobuf::int32 Ping::port() const {
  return port_;
}
inline void Ping::set_port(::google::protobuf::int32 value) {
  _set_bit(1);
  port_ = value;
}

// -------------------------------------------------------------------

// PingRes

// required int32 res = 1;
inline bool PingRes::has_res() const {
  return _has_bit(0);
}
inline void PingRes::clear_res() {
  res_ = 0;
  _clear_bit(0);
}
inline ::google::protobuf::int32 PingRes::res() const {
  return res_;
}
inline void PingRes::set_res(::google::protobuf::int32 value) {
  _set_bit(0);
  res_ = value;
}

// required bytes mess = 2;
inline bool PingRes::has_mess() const {
  return _has_bit(1);
}
inline void PingRes::clear_mess() {
  if (mess_ != &_default_mess_) {
    mess_->clear();
  }
  _clear_bit(1);
}
inline const ::std::string& PingRes::mess() const {
  return *mess_;
}
inline void PingRes::set_mess(const ::std::string& value) {
  _set_bit(1);
  if (mess_ == &_default_mess_) {
    mess_ = new ::std::string;
  }
  mess_->assign(value);
}
inline void PingRes::set_mess(const char* value) {
  _set_bit(1);
  if (mess_ == &_default_mess_) {
    mess_ = new ::std::string;
  }
  mess_->assign(value);
}
inline void PingRes::set_mess(const void* value, size_t size) {
  _set_bit(1);
  if (mess_ == &_default_mess_) {
    mess_ = new ::std::string;
  }
  mess_->assign(reinterpret_cast<const char*>(value), size);
}
inline ::std::string* PingRes::mutable_mess() {
  _set_bit(1);
  if (mess_ == &_default_mess_) {
    mess_ = new ::std::string;
  }
  return mess_;
}


// @@protoc_insertion_point(namespace_scope)

}  // namespace pink

#ifndef SWIG
namespace google {
namespace protobuf {


}  // namespace google
}  // namespace protobuf
#endif  // SWIG

// @@protoc_insertion_point(global_scope)

#endif  // PROTOBUF_pink_2eproto__INCLUDED