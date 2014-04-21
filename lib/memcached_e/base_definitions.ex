defmodule MemcachedE.BaseDefinitions do
  use Constants

  define protocol_binary_req, 0x80
  define protocol_binary_res, 0x81

  define protocol_binray_response_success, 0x00
  define protocol_binray_response_key_enoent, 0x01
  define protocol_binray_response_key_eexists, 0x02
  define protocol_binray_response_e2big, 0x03
  define protocol_binray_response_einval, 0x04
  define protocol_binray_response_not_stored, 0x05
  define protocol_binray_response_delta_badval, 0x06
  define protocol_binray_response_auth_error, 0x20
  define protocol_binray_response_auth_continue, 0x21
  define protocol_binray_response_unknown_command, 0x81
  define protocol_binray_response_enomem, 0x82

  # Defintion of the different command opcodes.
  # See section 3.3 Command Opcodes

  define protocol_binray_cmd_get, 0x00
  define protocol_binray_cmd_set, 0x01
  define protocol_binray_cmd_add, 0x02
  define protocol_binray_cmd_replace, 0x03
  define protocol_binray_cmd_delete, 0x04
  define protocol_binray_cmd_increment, 0x05
  define protocol_binray_cmd_decrement, 0x06
  define protocol_binray_cmd_quit, 0x07
  define protocol_binray_cmd_flush, 0x08
  define protocol_binray_cmd_getq, 0x09
  define protocol_binray_cmd_noop, 0x0a
  define protocol_binray_cmd_version, 0x0b
  define protocol_binray_cmd_getk, 0x0c
  define protocol_binray_cmd_getkq, 0x0d
  define protocol_binray_cmd_append, 0x0e
  define protocol_binray_cmd_prepend, 0x0f
  define protocol_binray_cmd_stat, 0x10
  define protocol_binray_cmd_setq, 0x11
  define protocol_binray_cmd_addq, 0x12
  define protocol_binray_cmd_replaceq, 0x13
  define protocol_binray_cmd_deleteq, 0x14
  define protocol_binray_cmd_incrementq, 0x15
  define protocol_binray_cmd_decrementq, 0x16
  define protocol_binray_cmd_quitq, 0x17
  define protocol_binray_cmd_flushq, 0x18
  define protocol_binray_cmd_appendq, 0x19
  define protocol_binray_cmd_prependq, 0x1a
  define protocol_binray_cmd_touch, 0x1c
  define protocol_binray_cmd_gat, 0x1d
  define protocol_binray_cmd_gatq, 0x1e
  define protocol_binray_cmd_gatk, 0x23
  define protocol_binray_cmd_gatkq, 0x24

  define protocol_binray_cmd_sasl_list_mechs, 0x20
  define protocol_binray_cmd_sasl_auth, 0x21
  define protocol_binray_cmd_sasl_step, 0x22

  # These commands are used for range operations and exist within
  # this header for use in other projects.  Range operations are
  # not expected to be implemented in the memcached server itself.

  define protocol_binray_cmd_rget, 0x30
  define protocol_binray_cmd_rset, 0x31
  define protocol_binray_cmd_rsetq, 0x32
  define protocol_binray_cmd_rappend, 0x33
  define protocol_binray_cmd_rappendq, 0x34
  define protocol_binray_cmd_rprepend, 0x35
  define protocol_binray_cmd_rprependq, 0x36
  define protocol_binray_cmd_rdelete, 0x37
  define protocol_binray_cmd_rdeleteq, 0x38
  define protocol_binray_cmd_rincr, 0x39
  define protocol_binray_cmd_rincrq, 0x3a
  define protocol_binray_cmd_rdecr, 0x3b
  define protocol_binray_cmd_rdecrq, 0x3c

  def send_data(server_state, data), do: server_state.transport.send(server_state.socket, data)

end
