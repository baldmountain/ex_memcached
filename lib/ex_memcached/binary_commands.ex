defmodule ExMemcached.BinaryCommands do
  require ExMemcached.BaseDefinitions
  alias ExMemcached.BaseDefinitions, as: Bd
  require Lager

  defp make_response_header(opcode, keylen, extlen, datatype, status, bodylen, opaque, cas \\ 0) do
    << Bd.protocol_binary_res, opcode, keylen::[big, size(16)], extlen, datatype, status::[big, size(16)], bodylen::[big, size(32)],
      opaque::[big, size(32)], cas::[big, size(64)] >>
  end

  def send_response_header(server_state, opcode, keylen, extlen, datatype, status, bodylen, opaque, cas \\ 0) do
    data = make_response_header(opcode, keylen, extlen, datatype, status, bodylen, opaque, cas)
    # Lager.info "send_response_header: #{inspect data}"
    Bd.send_data(server_state, data)
    server_state
  end

  def send_response_header_q(server_state, opcode, keylen, extlen, datatype, status, bodylen, opaque, cas \\ 0) do
    data = make_response_header(opcode, keylen, extlen, datatype, status, bodylen, opaque, cas)
    # Lager.info "send_response_header_q: #{inspect data}"
    server_state.stored_responses(server_state.stored_responses <> data)
  end

  def send_error(server_state, opcode, opaque, Bd.protocol_binray_response_key_enoent) do
    send_response_header(server_state, opcode, 0, 0, 0, Bd.protocol_binray_response_key_enoent, 9, opaque)
    Bd.send_data(server_state, <<"Not found">>)
    server_state
  end

  def send_error(server_state, opcode, opaque, Bd.protocol_binray_response_key_eexists) do
    send_response_header(server_state, opcode, 0, 0, 0, Bd.protocol_binray_response_key_eexists, 6, opaque)
    Bd.send_data(server_state, <<"Exists">>)
    server_state
  end

  def send_error(server_state, opcode, opaque, Bd.protocol_binray_response_e2big) do
    send_response_header(server_state, opcode, 0, 0, 0, Bd.protocol_binray_response_e2big, 7, opaque)
    Bd.send_data(server_state, <<"Too big">>)
    server_state
  end

  def send_error(server_state, opcode, opaque, Bd.protocol_binray_response_einval) do
    send_response_header(server_state, opcode, 0, 0, 0, Bd.protocol_binray_response_einval, 7, opaque)
    Bd.send_data(server_state, <<"Invalid">>)
    server_state
  end

  def send_error(server_state, opcode, opaque, Bd.protocol_binray_response_not_stored) do
    send_response_header(server_state, opcode, 0, 0, 0, Bd.protocol_binray_response_not_stored, 7, opaque)
    Bd.send_data(server_state, <<"Not stored">>)
    server_state
  end

  def send_error(server_state, opcode, opaque, Bd.protocol_binray_response_delta_badval) do
    send_response_header(server_state, opcode, 0, 0, 0, Bd.protocol_binray_response_delta_badval, 16, opaque)
    Bd.send_data(server_state, <<"Bad devlta value">>)
    server_state
  end

  def send_too_big_response_q(server_state, opcode, opaque) do
    Lager.info "data item too big for opcode: #{opcode}"
    send_response_header_q(server_state, opcode, 0, 0, 0, Bd.protocol_binray_response_e2big, 0, opaque)
  end

  def send_stored_responses server_state do
    cond do
      size(server_state.stored_responses) > 0 ->
        # Lager.info "Sending stored_responses #{size(server_state.stored_responses)}"
        Bd.send_data server_state, server_state.stored_responses
        server_state.stored_responses << >>
      true ->
        # Lager.info "sending nothing"
        server_state
    end
  end

  def binary_set_cmd(key, value, flags, exptime, opcode, opaque, cas, server_state) do
    # Lager.info "binary_set_cmd: #{key} #{inspect value} #{inspect flags} #{inspect exptime} 0x#{integer_to_binary(opaque, 16)} #{cas}"
    case ExMemcached.set(key, value, flags, exptime, cas) do
      {:stored, current_cas} -> send_response_header(server_state, opcode, 0, 0, 0, Bd.protocol_binray_response_success, 0, opaque, current_cas)
      :not_found -> send_error(server_state, opcode, opaque, Bd.protocol_binray_response_key_enoent)
      :exists -> send_error(server_state, opcode, opaque, Bd.protocol_binray_response_key_eexists)
      _ -> send_response_header(server_state, opcode, 0, 0, 0, Bd.protocol_binray_response_not_stored, 0, opaque, cas)
    end
  end

  def binary_add_cmd(key, value, flags, exptime, opcode, opaque, server_state) do
    # Lager.info "binary_add_cmd: #{key} #{inspect value} 0x#{integer_to_binary(opaque, 16)}"
    case ExMemcached.add(key, value, flags, exptime) do
      {:stored, current_cas} -> send_response_header(server_state, opcode, 0, 0, 0, Bd.protocol_binray_response_success, 0, opaque, current_cas)
      :not_stored -> send_error(server_state, opcode, opaque, Bd.protocol_binray_response_key_eexists)
    end
  end

  def binary_addq_cmd(key, value, flags, exptime, opcode, opaque, server_state) do
    # Lager.info "binary_addq_cmd: #{key} #{inspect value} 0x#{integer_to_binary(opaque, 16)}"
    case ExMemcached.add(key, value, flags, exptime) do
      :exists -> send_error(server_state, opcode, opaque, Bd.protocol_binray_response_key_eexists)
      _ -> server_state
    end
  end

  def binary_replace_cmd(key, value, flags, exptime, opcode, opaque, server_state) do
    # Lager.info "binary_replace_cmd: #{key} #{inspect value} 0x#{integer_to_binary(opaque, 16)}"
    case ExMemcached.replace(key, value, flags, exptime) do
      {:stored, current_cas} -> send_response_header(server_state, opcode, 0, 0, 0, Bd.protocol_binray_response_success, 0, opaque, current_cas)
      :not_stored -> send_error(server_state, opcode, opaque, Bd.protocol_binray_response_key_enoent)
    end
  end

  def binary_replaceq_cmd(key, value, flags, exptime, opcode, opaque, server_state) do
    # Lager.info "binary_replaceq_cmd: #{key} #{inspect value} 0x#{integer_to_binary(opaque, 16)}"
    case ExMemcached.replace(key, value, flags, exptime) do
      :not_found -> send_error(server_state, opcode, opaque, Bd.protocol_binray_response_key_enoent)
      _ -> server_state
    end
  end

  def binary_delete_cmd(key, opcode, opaque, server_state) do
    # Lager.info "binary_delete_cmd: #{key} 0x#{integer_to_binary(opaque, 16)}"
    case ExMemcached.delete(key) do
      :deleted -> send_response_header(server_state, opcode, 0, 0, 0, Bd.protocol_binray_response_success, 0, opaque)
      :not_found -> send_error(server_state, opcode, opaque, Bd.protocol_binray_response_key_enoent)
    end
  end

  def binary_deleteq_cmd(key, opcode, opaque, server_state) do
    # Lager.info "binary_deleteq_cmd: #{key} 0x#{integer_to_binary(opaque, 16)}"
    case ExMemcached.delete(key) do
      :not_found -> send_error(server_state, opcode, opaque, Bd.protocol_binray_response_key_enoent)
      _ -> server_state
    end
  end

  def binary_get_cmd(key, opcode, opaque, server_state) do
    # Lager.info "binary_get_cmd: #{key} 0x#{integer_to_binary(opaque, 16)}"
    case ExMemcached.get(key) do
      {value, flags, cas} when is_integer(value) ->
        send_response_header(server_state, opcode, 0, 4, 0, Bd.protocol_binray_response_success, 12, opaque, cas)
        Bd.send_data(server_state, << flags::size(32), value::size(64) >>)
      {value, flags, cas} when is_binary(value) ->
        len = size(value)
        if len > 0 do
          send_response_header(server_state, opcode, 0, 4, 0, Bd.protocol_binray_response_success, len + 4, opaque, cas)
          Bd.send_data(server_state, << flags::size(32) >> <> value)
        else
          send_response_header(server_state, opcode, 0, 4, 0, Bd.protocol_binray_response_success, 4, opaque, cas)
          Bd.send_data(server_state, << flags::size(32) >>)
        end
      :not_found -> send_error(server_state, opcode, opaque, Bd.protocol_binray_response_key_enoent)
    end
    server_state
  end

  def binary_getk_cmd(key, opcode, opaque, server_state) do
    # Lager.info "binary_getk_cmd: #{key} 0x#{integer_to_binary(opaque, 16)}"
    case ExMemcached.get(key) do
      {value, flags, cas} when is_integer(value) ->
        keylen = size(key)
        send_response_header(server_state, opcode, 0, 4, 0, Bd.protocol_binray_response_success, 12 + keylen, opaque, cas)
        Bd.send_data(server_state, << flags::size(32), value::size(64) >>)
      {value, flags, cas} when is_binary(value) ->
        len = size(value)
        keylen = size(key)
        if len > 0 do
          send_response_header(server_state, opcode, keylen, 4, 0, Bd.protocol_binray_response_success, len + keylen + 4, opaque, cas)
          Bd.send_data(server_state, << flags::size(32) >> <> key <> value)
        else
          send_response_header(server_state, opcode, keylen, 4, 0, Bd.protocol_binray_response_success, keylen + 4, opaque, cas)
          Bd.send_data(server_state, << flags::size(32) >> <> key)
        end
      :not_found -> send_error(server_state, opcode, opaque, Bd.protocol_binray_response_key_enoent)
    end
    server_state
  end

  def binary_getq_cmd(key, opcode, opaque, server_state) do
    # Lager.info "binary_getq_cmd: #{key} 0x#{integer_to_binary(opaque, 16)}"
    case ExMemcached.get(key) do
      {value, flags, cas} when is_integer(value) ->
        response = make_response_header(opcode, 0, 4, 0, Bd.protocol_binray_response_success, 12, opaque, cas)
          <> << flags::size(32), value::size(64) >>
        server_state.stored_responses(server_state.stored_responses <> response)
      {value, flags, cas} when is_binary(value) ->
        len = size(value)
        if len > 0 do
          response = make_response_header(opcode, 0, 4, 0, Bd.protocol_binray_response_success, len + 4, opaque, cas)
            <> << flags::size(32) >> <> value
        else
          response = make_response_header(opcode, 0, 4, 0, Bd.protocol_binray_response_success, 4, opaque, cas)
            <> << flags::size(32) >>
        end
        # Bd.send_data(server_state, response)
        # server_state
        server_state.stored_responses(server_state.stored_responses <> response)
      :not_found -> server_state
    end
  end

  def binary_getkq_cmd(key, opcode, opaque, server_state) do
    # Lager.info "binary_getkq_cmd: #{key} 0x#{integer_to_binary(opaque, 16)}"
    case ExMemcached.get(key) do
      {value, flags, cas} when is_integer(value) ->
        keylen = size(key)
        response = make_response_header(opcode, keylen, 4, 0, Bd.protocol_binray_response_success, 12 + keylen, opaque, cas)
          <> << flags::size(32) >> <> key <> << value::size(64) >>
        server_state.stored_responses(server_state.stored_responses <> response)
      {value, flags, cas} when is_binary(value) ->
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
        # Bd.send_data(server_state, response)
        # server_state
        server_state.stored_responses(server_state.stored_responses <> response)
      :not_found -> server_state
    end
  end

  def binary_gat_cmd(key, expirary, opcode, opaque, server_state) do
    # Lager.info "binary_gat_cmd: #{expirary} #{key} 0x#{integer_to_binary(opaque, 16)}"
    case ExMemcached.gat(key, expirary) do
      {value, flags, cas} when is_integer(value) ->
        send_response_header(server_state, opcode, 0, 4, 0, Bd.protocol_binray_response_success, 12, opaque, cas)
        Bd.send_data(server_state, << flags::size(32), value::size(64) >>)
      {value, flags, cas} when is_binary(value) ->
        len = size(value)
        if len > 0 do
          send_response_header(server_state, opcode, 0, 4, 0, Bd.protocol_binray_response_success, len + 4, opaque, cas)
          Bd.send_data(server_state, << flags::size(32) >> <> value)
        else
          send_response_header(server_state, opcode, 0, 4, 0, Bd.protocol_binray_response_success, 4, opaque, cas)
          Bd.send_data(server_state, << flags::size(32) >>)
        end
      :not_found -> send_error(server_state, opcode, opaque, Bd.protocol_binray_response_key_enoent)
    end
    server_state
  end

  def binary_incr_cmd(key, count, intial, expiration, opcode, opaque, server_state) do
    # Lager.info "binary_incr_cmd: #{key} #{inspect count} 0x#{integer_to_binary(opaque, 16)}"
    case ExMemcached.incr(key, count, intial, expiration) do
      :not_found -> send_error(server_state, opcode, opaque, Bd.protocol_binray_response_key_enoent)
      :invalid_incr_decr -> send_error(server_state, opcode, opaque, Bd.protocol_binray_response_delta_badval)
      {value, cas} ->
        send_response_header(server_state, opcode, 0, 0, 0, Bd.protocol_binray_response_success, 8, opaque, cas)
        Bd.send_data(server_state, << value::size(64) >>)
    end
    server_state
  end

  def binary_incrq_cmd(key, count, intial, expiration, opcode, opaque, server_state) do
    # Lager.info "binary_incrq_cmd: #{key} #{inspect count}"
    case ExMemcached.incr(key, count, intial, expiration) do
      :not_found ->
        response = make_response_header(opcode, 0, 0, 0, Bd.protocol_binray_response_key_enoent, 0, opaque)
        server_state.stored_responses(server_state.stored_responses <> response)
      _ -> server_state
    end
  end

  def binary_decr_cmd(key, count, intial, expiration, opcode, opaque, server_state) do
    # Lager.info "binary_decr_cmd: #{key} #{inspect count} 0x#{integer_to_binary(opaque, 16)}"
    case ExMemcached.decr(key, count, intial, expiration) do
      :not_found -> send_error(server_state, opcode, opaque, Bd.protocol_binray_response_key_enoent)
      :invalid_incr_decr -> send_error(server_state, opcode, opaque, Bd.protocol_binray_response_delta_badval)
      {value, cas} ->
        send_response_header(server_state, opcode, 0, 0, 0, Bd.protocol_binray_response_success, 8, opaque, cas)
        Bd.send_data(server_state, << value::size(64) >>)
    end
    server_state
  end

  def binary_decrq_cmd(key, count, intial, expiration, opcode, opaque, server_state) do
    # Lager.info "binary_decrq_cmd: #{key} #{inspect count}"
    case ExMemcached.decr(key, count, intial, expiration) do
      :not_found ->
        response = make_response_header(opcode, 0, 0, 0, Bd.protocol_binray_response_key_enoent, 0, opaque)
        server_state.stored_responses(server_state.stored_responses <> response)
      _ -> server_state
    end

  end

  def binary_version_cmd(opcode, opaque, server_state) do
    # Lager.info "binary_version_cmd: 0x#{integer_to_binary(opaque, 16)}"
    {:ok, state} = :application.get_all_key :ex_memcached
    data = <<"#{state[:vsn]}">>
    send_response_header(server_state, opcode, 0, 0, 0, Bd.protocol_binray_response_success, size(data), opaque)
    Bd.send_data(server_state, data)
    server_state
  end

  def binary_flush_cmd(expiration, opcode, opaque, server_state) do
    # Lager.info "binary_flush_cmd: 0x#{integer_to_binary(opaque, 16)}"
    :ok = ExMemcached.flush expiration
    send_response_header(server_state, opcode, 0, 0, 0, Bd.protocol_binray_response_success, 0, opaque)
  end

  def binary_flushq_cmd(expiration, _opcode, _opaque, server_state) do
    # Lager.info "binary_flushq_cmd:"
    :ok = ExMemcached.flush expiration
    server_state
  end

  def binary_append_cmd(key, value, opcode, opaque, server_state) do
    # Lager.info "binary_append_cmd: #{key} #{inspect value} 0x#{integer_to_binary(opaque, 16)}"
    case ExMemcached.append(key, value, 0, 0) do
      {:stored, current_cas} -> send_response_header(server_state, opcode, 0, 0, 0, Bd.protocol_binray_response_success, 0, opaque, current_cas)
      _ -> send_error(server_state, opcode, opaque, Bd.protocol_binray_response_not_stored)
    end
    server_state
  end

  def binary_appendq_cmd(key, value, opcode, opaque, server_state) do
    # Lager.info "binary_appendq_cmd: #{key} #{inspect value} 0x#{integer_to_binary(opaque, 16)}"
    case ExMemcached.append(key, value, 0, 0) do
      {:stored, _current_cas} -> server_state
      _ ->
        send_response_header(server_state, opcode, 0, 0, 0, Bd.protocol_binray_response_not_stored, 0, opaque)
    end
  end

  def binary_prepend_cmd(key, value, opcode, opaque, server_state) do
    # Lager.info "binary_prepend_cmd: #{key} #{inspect value} 0x#{integer_to_binary(opaque, 16)}"
    case ExMemcached.prepend(key, value, 0, 0) do
      {:stored, current_cas} -> send_response_header(server_state, opcode, 0, 0, 0, Bd.protocol_binray_response_success, 0, opaque, current_cas)
      _ -> send_response_header(server_state, opcode, 0, 0, 0, Bd.protocol_binray_response_not_stored, 0, opaque)
    end
  end

  def binary_prependq_cmd(key, value, opcode, opaque, server_state) do
    # Lager.info "binary_prependq_cmd: #{key} #{inspect value} 0x#{integer_to_binary(opaque, 16)}"
    case ExMemcached.prepend(key, value, 0, 0) do
      {:stored, _current_cas} -> server_state
      _ ->
        send_response_header(server_state, opcode, 0, 0, 0, Bd.protocol_binray_response_not_stored, 0, opaque)
    end
  end

  def binary_touch_cmd(key, exptime, opcode, opaque, server_state) do
    # Lager.info "binary_touch_cmd: #{key} #{exptime} 0x#{integer_to_binary(opaque, 16)}"
    case ExMemcached.touch(key, exptime) do
      :touched -> send_response_header(server_state, opcode, 0, 0, 0, Bd.protocol_binray_response_success, 0, opaque)
      :not_found -> send_error(server_state, opcode, opaque, Bd.protocol_binray_response_key_enoent)
    end
  end
end
