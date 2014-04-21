defmodule MemcachedE.BinaryCommands do
  require MemcachedE.BaseDefinitions
  alias MemcachedE.BaseDefinitions, as: Bd

  defp make_response_header(opcode, keylen, extlen, datatype, status, bodylen, opaque, cas \\ 0) do
    << Bd.protocol_binary_res, opcode, keylen::[big, size(16)], extlen, datatype, status::[big, size(16)], bodylen::[big, size(32)],
      opaque::[big, size(32)], cas::[big, size(64)] >>
  end

  def send_response_header(server_state, opcode, keylen, extlen, datatype, status, bodylen, opaque, cas \\ 0) do
    data = make_response_header(opcode, keylen, extlen, datatype, status, bodylen, opaque, cas)
    # Lager.info "send_response_header: #{inspect data}"
    Bd.send_data(server_state, data)
  end

  def binary_set_cmd(key, value, opcode, opaque, server_state) do
    server_state = send_saved_responses(server_state)
    case MemcachedE.set(key, value, 0, 0) do
      {:stored, current_cas} -> send_response_header(server_state, opcode, 0, 0, 0, Bd.protocol_binray_response_success, 0, opaque, current_cas)
      _ -> send_response_header(server_state, opcode, 0, 0, 0, Bd.protocol_binray_response_not_stored, 0, opaque)
    end
    server_state
  end

  def binary_add_cmd(key, value, opcode, opaque, server_state) do
    # Lager.info ">> #{inspect key} - #{inspect value}"
    server_state = send_saved_responses(server_state)
    case MemcachedE.add(key, value, 0, 0) do
      {:stored, current_cas} -> send_response_header(server_state, opcode, 0, 0, 0, Bd.protocol_binray_response_success, 0, opaque, current_cas)
      :exists -> send_response_header(server_state, opcode, 0, 0, 0, Bd.protocol_binray_response_key_eexists, 0, opaque)
    end
    server_state
  end

  def binary_addq_cmd(key, value, opcode, opaque, server_state) do
    case MemcachedE.add(key, value, 0, 0) do
      :exists -> send_response_header(server_state, opcode, 0, 0, 0, Bd.protocol_binray_response_key_eexists, 0, opaque)
      _ -> :ok
    end
    server_state
  end

  def binary_replace_cmd(key, value, opcode, opaque, server_state) do
    server_state = send_saved_responses(server_state)
    case MemcachedE.replace(key, value, 0, 0) do
      {:stored, current_cas} -> send_response_header(server_state, opcode, 0, 0, 0, Bd.protocol_binray_response_success, 0, opaque, current_cas)
      :not_found -> send_response_header(server_state, opcode, 0, 0, 0, Bd.protocol_binray_response_key_enoent, 0, opaque)
    end
    server_state
  end

  def binary_replaceq_cmd(key, value, opcode, opaque, server_state) do
    case MemcachedE.replace(key, value, 0, 0) do
      :not_found -> send_response_header(server_state, opcode, 0, 0, 0, Bd.protocol_binray_response_key_enoent, 0, opaque)
      _ -> :ok
    end
    server_state
  end

  def binary_delete_cmd(key, opcode, opaque, server_state) do
    server_state = send_saved_responses(server_state)
    case MemcachedE.delete(key) do
      :deleted -> send_response_header(server_state, opcode, 0, 0, 0, Bd.protocol_binray_response_success, 0, opaque)
      :not_found -> send_response_header(server_state, opcode, 0, 0, 0, Bd.protocol_binray_response_key_enoent, 0, opaque)
    end
    server_state
  end

  def binary_deleteq_cmd(key, opcode, opaque, server_state) do
    case MemcachedE.delete(key) do
      :not_found -> send_response_header(server_state, opcode, 0, 0, 0, Bd.protocol_binray_response_key_enoent, 0, opaque)
      _ -> :ok
    end
    server_state
  end

  def binary_get_cmd(key, opcode, opaque, server_state) do
    server_state = send_saved_responses(server_state)
    case MemcachedE.get(key) do
      {value, flags, cas} ->
        len = size(value)
        if len > 0 do
          send_response_header(server_state, opcode, 0, 4, 0, Bd.protocol_binray_response_success, len + 4, opaque, cas)
          Bd.send_data(server_state, << flags::size(32) >> <> value)
        else
          send_response_header(server_state, opcode, 0, 4, 0, Bd.protocol_binray_response_success, 4, opaque, cas)
          Bd.send_data(server_state, << flags::size(32) >>)
        end
      :not_found -> send_response_header(server_state, opcode, 0, 0, 0, Bd.protocol_binray_response_key_enoent, 0, opaque)
    end
    server_state
  end

  def binary_getk_cmd(key, opcode, opaque, server_state) do
    server_state = send_saved_responses(server_state)
    case MemcachedE.get(key) do
      {value, flags, cas} ->
        len = size(value)
        keylen = size(key)
        extlen = 4
        if len > 0 do
          send_response_header(server_state, opcode, keylen, 4, 0, Bd.protocol_binray_response_success, len + keylen + extlen, opaque, cas)
          Bd.send_data(server_state, << flags::size(32) >> <> key <> value)
        else
          send_response_header(server_state, opcode, keylen, 4, 0, Bd.protocol_binray_response_success, keylen + extlen, opaque, cas)
          Bd.send_data(server_state, << flags::size(32) >> <> key)
        end
      :not_found -> send_response_header(server_state, opcode, 0, 0, 0, Bd.protocol_binray_response_key_enoent, 0, opaque)
    end
    server_state
  end

  def binary_getq_cmd(key, opcode, opaque, server_state) do
    # Lager.info "get key: #{key}"
    case MemcachedE.get(key) do
      {value, flags, cas} ->
        len = size(value)
        if len > 0 do
          response = make_response_header(opcode, 0, 4, 0, Bd.protocol_binray_response_success, len + 4, opaque, cas)
            <> << flags::size(32) >> <> value
        else
          response = make_response_header(opcode, 0, 4, 0, Bd.protocol_binray_response_success, 4, opaque, cas)
            <> << flags::size(32) >>
        end
        Bd.send_data(server_state, response)
      :not_found -> :ok # send_response_header(server_state, opcode, 0, 0, 0, Bd.protocol_binray_response_key_enoent, 0, opaque)
    end
    server_state
  end

  def binary_getkq_cmd(key, opcode, opaque, server_state) do
    # Lager.info "get key: #{key}"
    case MemcachedE.get(key) do
      {value, flags, cas} ->
        len = size(value)
        keylen = size(key)
        extlen = 4
        if len > 0 do
          response = make_response_header(opcode, keylen, 4, 0, Bd.protocol_binray_response_success, len + keylen + extlen, opaque, cas)
            <> << flags::size(32) >> <> key <> value
        else
          response = make_response_header(opcode, keylen, 4, 0, Bd.protocol_binray_response_success, keylen + extlen, opaque, cas)
            <> << flags::size(32) >> <> key
        end
        Bd.send_data(server_state, response)
      :not_found -> :ok # send_response_header(server_state, opcode, 0, 0, 0, Bd.protocol_binray_response_key_enoent, 0, opaque)
    end
    server_state
  end

  def binary_incr_cmd(key, count, intial, expiration, opcode, opaque, server_state) do
    # Lager.info ">> #{inspect key} - #{inspect value}"
    server_state = send_saved_responses(server_state)
    case MemcachedE.incr(key, count, intial, expiration) do
      :not_found -> send_response_header(server_state, opcode, 0, 0, 0, Bd.protocol_binray_response_key_enoent, 0, opaque)
      {value, cas} ->
        send_response_header(server_state, opcode, 0, 0, 0, Bd.protocol_binray_response_success, 8, opaque, cas)
        Bd.send_data(server_state, << value::size(64) >>)
    end
    server_state
  end

  defp send_saved_responses(server_state) do
    Enum.each(server_state.saved_responses, fn (response) ->
      Bd.send_data(server_state, response)
    end)
    server_state.saved_responses []
  end
end
