defmodule MemcachedE.Server do
  use GenServer
  require Lager

  @protocol_binary_req 0x80
  @protocol_binary_res 0x81

  @protocol_binray_response_success 0x00
  @protocol_binray_response_key_enoent 0x01
  @protocol_binray_response_key_eexists 0x02
  @protocol_binray_response_e2big 0x03
  @protocol_binray_response_einval 0x04
  @protocol_binray_response_not_stored 0x05
  @protocol_binray_response_delta_badval 0x06
  @protocol_binray_response_auth_error 0x20
  @protocol_binray_response_auth_continue 0x21
  @protocol_binray_response_unknown_command 0x81
  @protocol_binray_response_enomem 0x82

  # Defintion of the different command opcodes.
  # See section 3.3 Command Opcodes

  @protocol_binray_cmd_get 0x00
  @protocol_binray_cmd_set 0x01
  @protocol_binray_cmd_add 0x02
  @protocol_binray_cmd_replace 0x03
  @protocol_binray_cmd_delete 0x04
  @protocol_binray_cmd_increment 0x05
  @protocol_binray_cmd_decrement 0x06
  @protocol_binray_cmd_quit 0x07
  @protocol_binray_cmd_flush 0x08
  @protocol_binray_cmd_getq 0x09
  @protocol_binray_cmd_noop 0x0a
  @protocol_binray_cmd_version 0x0b
  @protocol_binray_cmd_getk 0x0c
  @protocol_binray_cmd_getkq 0x0d
  @protocol_binray_cmd_append 0x0e
  @protocol_binray_cmd_prepend 0x0f
  @protocol_binray_cmd_stat 0x10
  @protocol_binray_cmd_setq 0x11
  @protocol_binray_cmd_addq 0x12
  @protocol_binray_cmd_replaceq 0x13
  @protocol_binray_cmd_deleteq 0x14
  @protocol_binray_cmd_incrementq 0x15
  @protocol_binray_cmd_decrementq 0x16
  @protocol_binray_cmd_quitq 0x17
  @protocol_binray_cmd_flushq 0x18
  @protocol_binray_cmd_appendq 0x19
  @protocol_binray_cmd_prependq 0x1a
  @protocol_binray_cmd_touch 0x1c
  @protocol_binray_cmd_gat 0x1d
  @protocol_binray_cmd_gatq 0x1e
  @protocol_binray_cmd_gatk 0x23
  @protocol_binray_cmd_gatkq 0x24

  @protocol_binray_cmd_sasl_list_mechs 0x20
  @protocol_binray_cmd_sasl_auth 0x21
  @protocol_binray_cmd_sasl_step 0x22

  # These commands are used for range operations and exist within
  # this header for use in other projects.  Range operations are
  # not expected to be implemented in the memcached server itself.

  @protocol_binray_cmd_rget 0x30
  @protocol_binray_cmd_rset 0x31
  @protocol_binray_cmd_rsetq 0x32
  @protocol_binray_cmd_rappend 0x33
  @protocol_binray_cmd_rappendq 0x34
  @protocol_binray_cmd_rprepend 0x35
  @protocol_binray_cmd_rprependq 0x36
  @protocol_binray_cmd_rdelete 0x37
  @protocol_binray_cmd_rdeleteq 0x38
  @protocol_binray_cmd_rincr 0x39
  @protocol_binray_cmd_rincrq 0x3a
  @protocol_binray_cmd_rdecr 0x3b
  @protocol_binray_cmd_rdecrq 0x3c

  defrecord ServerState, socket: nil, transport: nil, existing_data: "", saved_responses: []

  def start_link(ref, socket, transport, opts) do
    pid = spawn_link(__MODULE__, :init, [ref, socket, transport, opts])
    {:ok, pid}
  end

  def init(ref, socket, transport, _opts \\ []) do
    :ok = :ranch.accept_ack(ref)
    loop(ServerState[socket: socket, transport: transport])
  end

  defp loop(server_state) do
    # Lager.info "calling receive"
  	case server_state.transport.recv(server_state.socket, 0, 5000) do
  		{:ok, data} ->
        data = server_state.existing_data <> data
        if String.starts_with?(data, << @protocol_binary_req >>) do
          server_state = handle_binary_protocol(data, server_state)
        else
          server_state = handle_ascii_protocol(data, server_state)
        end
  			loop(server_state)
  		res ->
        Lager.info "loop res: #{inspect res}"
  			:ok = close_transport(server_state)
  	end
  end

  defp handle_binary_protocol(<<>>, server_state) do
    server_state
  end

  defp handle_binary_protocol(data, server_state) do
    << magic, opcode, keylen::[big, unsigned, integer, size(16)], extlen, datatype, reserved::[big, unsigned, integer, size(16)],
      bodylen::[big, unsigned, integer, size(32)], opaque::[big, unsigned, integer, size(32)], cas::[big, unsigned, integer, size(64)], tail::binary >> = data
      Lager.info "magic #{magic} opcode #{opcode} keylen #{keylen} extlen #{extlen} datatype #{datatype} reserved #{reserved} bodylen #{bodylen} opaque #{opaque} cas #{cas}"
    case opcode do
      @protocol_binray_cmd_noop -> send_response_header(server_state, opcode, 0, 0, 0, @protocol_binray_response_success, 0, opaque)
      @protocol_binray_cmd_quit ->
        send_response_header(server_state, opcode, 0, 0, 0, @protocol_binray_response_success, 0, opaque)
        close_transport(server_state)
      @protocol_binray_cmd_quitq ->
        close_transport(server_state)
      @protocol_binray_cmd_set ->
        {key, data} = key_data(extlen, keylen, tail)
        data = read_remainder(data, bodylen - extlen - keylen, server_state)
        case read_remainder(data, bodylen - extlen - keylen, server_state) do
          {data, rest} ->
            server_state = binary_set_cmd(key, data, opcode, opaque, server_state)
            server_state = handle_binary_protocol(rest, server_state)
          data -> server_state = binary_set_cmd(key, data, opcode, opaque, server_state)
        end
      @protocol_binray_cmd_setq ->
        {key, data} = key_data(extlen, keylen, tail)
        case read_remainder(data, bodylen - extlen - keylen, server_state) do
          {data, rest} ->
            MemcachedE.set(key, data, 0, 0)
            server_state = handle_binary_protocol(rest, server_state)
          data -> MemcachedE.set(key, data, 0, 0)
        end
      @protocol_binray_cmd_add ->
        {key, data} = key_data(extlen, keylen, tail)
        case read_remainder(data, bodylen - extlen - keylen, server_state) do
          {data, rest} ->
            server_state = binary_add_cmd(key, data, opcode, opaque, server_state)
            server_state = handle_binary_protocol(rest, server_state)
          data -> binary_add_cmd(key, data, opcode, opaque, server_state)
        end
      @protocol_binray_cmd_addq ->
        {key, data} = key_data(extlen, keylen, tail)
        case read_remainder(data, bodylen - extlen - keylen, server_state) do
          {data, rest} ->
            server_state = binary_addq_cmd(key, data, opcode, opaque, server_state)
            server_state = handle_binary_protocol(rest, server_state)
          data -> server_state = binary_addq_cmd(key, data, opcode, opaque, server_state)
        end
      @protocol_binray_cmd_replace ->
        {key, data} = key_data(extlen, keylen, tail)
        case read_remainder(data, bodylen - extlen - keylen, server_state) do
          {data, rest} ->
            server_state = binary_replace_cmd(key, data, opcode, opaque, server_state)
            server_state = handle_binary_protocol(rest, server_state)
          data -> server_state = binary_replace_cmd(key, data, opcode, opaque, server_state)
        end
      @protocol_binray_cmd_replaceq ->
        {key, data} = key_data(extlen, keylen, tail)
        case read_remainder(data, bodylen - extlen - keylen, server_state) do
          {data, rest} ->
            server_state = binary_replaceq_cmd(key, data, opcode, opaque, server_state)
            server_state = handle_binary_protocol(rest, server_state)
          data -> server_state = binary_replaceq_cmd(key, data, opcode, opaque, server_state)
        end
      @protocol_binray_cmd_delete ->
        {key, data} = key_data(extlen, keylen, tail)
        case read_remainder(data, bodylen - extlen - keylen, server_state) do
          {_, rest} ->
            server_state = binary_delete_cmd(key, opcode, opaque, server_state)
            server_state = handle_binary_protocol(rest, server_state)
          _ -> server_state = binary_delete_cmd(key, opcode, opaque, server_state)
        end
      @protocol_binray_cmd_deleteq ->
        {key, data} = key_data(extlen, keylen, tail)
        server_state = binary_deleteq_cmd(key, opcode, opaque, server_state)
      @protocol_binray_cmd_get ->
        {key, data} = key_data(extlen, keylen, tail)
        server_state = binary_get_cmd(key, opcode, opaque, server_state)
        server_state = handle_binary_protocol(data, server_state)
      @protocol_binray_cmd_getq ->
        {key, data} = key_data(extlen, keylen, tail)
        server_state = binary_getq_cmd(key, opcode, opaque, server_state)
        server_state = handle_binary_protocol(data, server_state)
      @protocol_binray_cmd_getk ->
        {key, data} = key_data(extlen, keylen, tail)
        server_state = binary_getk_cmd(key, opcode, opaque, server_state)
        server_state = handle_binary_protocol(data, server_state)
      @protocol_binray_cmd_getkq ->
        {key, data} = key_data(extlen, keylen, tail)
        server_state = binary_getkq_cmd(key, opcode, opaque, server_state)
        server_state = handle_binary_protocol(data, server_state)
      _ -> Lager.info "Unknown opcode: #{opcode}"
    end
    server_state
  end

  defrecord LoopState, state: :commands, key: nil, flags: 0, exptime: 0, data_length: 0, no_reply: nil

  defp handle_ascii_protocol(data, server_state) do
    if String.ends_with?(data, "\r\n") do
      cmds = String.split data, "\r\n"
      server_state = server_state.existing_data ""
    else
      cmds = String.split data, "\r\n"
      server_state = server_state.existing_data List.last(cmds)
    end
    cmds = List.delete_at(cmds, -1)

    Enum.reduce cmds, LoopState.new, fn(cmd, loop_state) ->
      command = if (loop_state.state == :commands) do
        parts = String.split cmd, " "
         List.first(parts)
      end
      cond do
        loop_state.state == :commands && (command == "get"  || command == "gets") ->
          [_|tail] = parts
          get_cmd(tail, server_state)
          no_reply = nil
        loop_state.state == :commands && command == "set" ->
          case parts do
            [_, k, flags, exptime, data_length] ->
              loop_state = loop_state.flags binary_to_integer(flags)
              loop_state = loop_state.exptime binary_to_integer(exptime)
              loop_state = loop_state.data_length binary_to_integer(data_length)
              loop_state = loop_state.key k
              loop_state = loop_state.state :set
            [_, k, flags, exptime, data_length, nr] ->
              loop_state = loop_state.flags binary_to_integer(flags)
              loop_state = loop_state.exptime binary_to_integer(exptime)
              loop_state = loop_state.data_length binary_to_integer(data_length)
              loop_state = loop_state.key k
              loop_state = loop_state.state :set
              loop_state = loop_state.no_reply nr
            _ ->
              send_error(server_state)
              loop_state = loop_state.state :commands
          end
          # Lager.info "set key: #{loop_state.key} flags: #{loop_state.flags} exptime: #{loop_state.exptime} data_length: #{loop_state.data_length} no_reply: #{loop_state.no_reply}"
        loop_state.state == :set && size(cmd) == loop_state.data_length ->
          set_cmd(loop_state, cmd, server_state)
          loop_state = loop_state.state :commands
          loop_state = loop_state.no_reply nil
        loop_state.state == :commands && command == "delete"->
          [_|tail] = parts
          delete_cmd(tail, server_state)
          loop_state = loop_state.no_reply nil
        loop_state.state == :commands && command == "incr"->
          [_|tail] = parts
          incr_cmd(tail, server_state)
          loop_state = loop_state.no_reply nil
        loop_state.state == :commands && command == "decr"->
          [_|tail] = parts
          decr_cmd(tail, server_state)
          loop_state = loop_state.no_reply nil
        loop_state.state == :commands && command == "stats"->
          stats_cmd(parts, server_state)
          loop_state = loop_state.no_reply nil
        loop_state.state == :set ->
          Lager.info "bad data length"
          send_error(server_state)
          loop_state = loop_state.state :commands
          loop_state = loop_state.no_reply nil
        size(cmd) == 0 ->
          send_data(server_state, <<"END\r\n">>)
        true ->
          Lager.info "unknown command: #{cmd} in state: #{loop_state.state}"
          loop_state = loop_state.state :commands
          close_transport(server_state)
      end
      # Lager.info "state: #{state} key: #{key} flags: #{flags} exptime: #{exptime} data_length: #{data_length} no_reply: #{no_reply}"
      loop_state
    end
    server_state
  end

  defp send_error(server_state) do
    send_data(server_state, <<"ERROR\r\n">>)
  end

  defp set_cmd(loop_state = LoopState[no_reply: nil], value, server_state) do
    case MemcachedE.set(loop_state.key, value, loop_state.flags, loop_state.exptime) do
      {:stored, _} -> send_data(server_state, <<"STORED\r\n">>)
      :not_stored -> send_data(server_state, <<"NOT_STORED\r\n">>)
      :exists -> send_data(server_state, <<"EXISTS\r\n">>)
      :not_found -> send_data(server_state, <<"NOT_FOUND\r\n">>)
      true -> send_error(server_state)
    end
  end

  defp set_cmd(loop_state, value, _server_state) do
    MemcachedE.set(loop_state.key, value, loop_state.flags, loop_state.exptime)
  end

  defp get_cmd([key|tail], server_state) do
    # Lager.info "get_cmd: #{key}"
    case MemcachedE.get key do
      :not_found ->
        :ok # skip unfound values
      {value, flags, cas} ->
        data = <<"VALUE #{key} #{flags} #{size(value)} #{cas}\r\n">>
        send_data(server_state, data)
        send_data(server_state, value <> <<"\r\n">>)
    end
    get_cmd tail, server_state
  end

  defp get_cmd([], server_state) do
    send_data(server_state, <<"END\r\n">>)
  end

  defp delete_cmd([key], server_state) do
    case MemcachedE.delete(key) do
      :deleted -> send_data(server_state, <<"DELETED\r\n">>)
      :not_found -> send_data(server_state, <<"NOT_FOUND\r\n">>)
      true -> send_error(server_state)
    end
  end

  defp delete_cmd([key, _], _server_state) do
    MemcachedE.delete(key)
  end

  defp incr_cmd([key], server_state) do
    case MemcachedE.incr(key, 1) do
      :not_found -> send_data(server_state, <<"NOT_FOUND\r\n">>)
      value when is_binary(value) -> send_data(server_state, value <> <<"\r\n">>)
      true -> send_error(server_state)
    end
  end

  defp incr_cmd([key, count], server_state) do
    case MemcachedE.incr(key, binary_to_integer(count)) do
      :not_found -> send_data(server_state, <<"NOT_FOUND\r\n">>)
      value when is_binary(value) -> send_data(server_state, value <> <<"\r\n">>)
      true -> send_error(server_state)
    end
  end

  defp incr_cmd([key, count, _], _server_state) do
    MemcachedE.incr(key, binary_to_integer(count))
  end

  defp decr_cmd([key], server_state) do
    case MemcachedE.decr(key, 1) do
      :not_found -> send_data(server_state, <<"NOT_FOUND\r\n">>)
      value when is_binary(value) -> send_data(server_state, value <> <<"\r\n">>)
      true -> send_error(server_state)
    end
  end

  defp decr_cmd([key, count], server_state) do
    case MemcachedE.decr(key, binary_to_integer(count)) do
      :not_found -> send_data(server_state, <<"NOT_FOUND\r\n">>)
      value when is_binary(value) -> send_data(server_state, value <> <<"\r\n">>)
      true -> send_error(server_state)
    end
  end

  defp decr_cmd([key, count, _], _server_state) do
    MemcachedE.incr(key, binary_to_integer(count))
  end

  defp stats_cmd(["stats","cachedump","1", "0", "0"], server_state) do
    send_data(server_state, <<"END\r\n">>)
  end

  defp stats_cmd(["stats","cachedump","200", "0", "0"], server_state) do
    send_data(server_state, <<"CLIENT_ERROR\r\n">>)
  end

  defp make_response_header(server_state, opcode, keylen, extlen, datatype, status, bodylen, opaque, cas \\ 0) do
    << @protocol_binary_res, opcode, keylen::[big, size(16)], extlen, datatype, status::[big, size(16)], bodylen::[big, size(32)],
      opaque::[big, size(32)], cas::[big, size(64)] >>
  end

  defp send_response_header(server_state, opcode, keylen, extlen, datatype, status, bodylen, opaque, cas \\ 0) do
    data = make_response_header(server_state, opcode, keylen, extlen, datatype, status, bodylen, opaque, cas)
    # Lager.info "send_response_header: #{inspect data}"
    send_data(server_state, data)
  end

  def binary_set_cmd(key, value, opcode, opaque, server_state) do
    server_state = send_saved_responses(server_state)
    case MemcachedE.set(key, value, 0, 0) do
      {:stored, current_cas} -> send_response_header(server_state, opcode, 0, 0, 0, @protocol_binray_response_success, 0, opaque, current_cas)
      _ -> send_response_header(server_state, opcode, 0, 0, 0, @protocol_binray_response_not_stored, 0, opaque)
    end
    server_state
  end

  def binary_add_cmd(key, value, opcode, opaque, server_state) do
    # Lager.info ">> #{inspect key} - #{inspect value}"
    server_state = send_saved_responses(server_state)
    case MemcachedE.add(key, value, 0, 0) do
      {:stored, current_cas} -> send_response_header(server_state, opcode, 0, 0, 0, @protocol_binray_response_success, 0, opaque, current_cas)
      :exists -> send_response_header(server_state, opcode, 0, 0, 0, @protocol_binray_response_key_eexists, 0, opaque)
    end
    server_state
  end

  def binary_addq_cmd(key, value, opcode, opaque, server_state) do
    case MemcachedE.add(key, value, 0, 0) do
      :exists -> send_response_header(server_state, opcode, 0, 0, 0, @protocol_binray_response_key_eexists, 0, opaque)
      _ -> :ok
    end
    server_state
  end

  def binary_replace_cmd(key, value, opcode, opaque, server_state) do
    server_state = send_saved_responses(server_state)
    case MemcachedE.replace(key, value, 0, 0) do
      {:stored, current_cas} -> send_response_header(server_state, opcode, 0, 0, 0, @protocol_binray_response_success, 0, opaque, current_cas)
      :not_found -> send_response_header(server_state, opcode, 0, 0, 0, @protocol_binray_response_key_enoent, 0, opaque)
    end
    server_state
  end

  def binary_replaceq_cmd(key, value, opcode, opaque, server_state) do
    case MemcachedE.replace(key, value, 0, 0) do
      :not_found -> send_response_header(server_state, opcode, 0, 0, 0, @protocol_binray_response_key_enoent, 0, opaque)
      _ -> :ok
    end
    server_state
  end

  def binary_delete_cmd(key, opcode, opaque, server_state) do
    server_state = send_saved_responses(server_state)
    case MemcachedE.delete(key) do
      :deleted -> send_response_header(server_state, opcode, 0, 0, 0, @protocol_binray_response_success, 0, opaque)
      :not_found -> send_response_header(server_state, opcode, 0, 0, 0, @protocol_binray_response_key_enoent, 0, opaque)
    end
    server_state
  end

  def binary_deleteq_cmd(key, opcode, opaque, server_state) do
    case MemcachedE.delete(key) do
      :not_found -> send_response_header(server_state, opcode, 0, 0, 0, @protocol_binray_response_key_enoent, 0, opaque)
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
          send_response_header(server_state, opcode, 0, 4, 0, @protocol_binray_response_success, len + 4, opaque, cas)
          send_data(server_state, << flags::size(32) >> <> value)
        else
          send_response_header(server_state, opcode, 0, 4, 0, @protocol_binray_response_success, 4, opaque, cas)
          send_data(server_state, << flags::size(32) >>)
        end
      :not_found -> send_response_header(server_state, opcode, 0, 0, 0, @protocol_binray_response_key_enoent, 0, opaque)
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
          send_response_header(server_state, opcode, keylen, 4, 0, @protocol_binray_response_success, len + keylen + extlen, opaque, cas)
          send_data(server_state, << flags::size(32) >> <> key <> value)
        else
          send_response_header(server_state, opcode, keylen, 4, 0, @protocol_binray_response_success, keylen + extlen, opaque, cas)
          send_data(server_state, << flags::size(32) >> <> key)
        end
      :not_found -> send_response_header(server_state, opcode, 0, 0, 0, @protocol_binray_response_key_enoent, 0, opaque)
    end
    server_state
  end

  def binary_getq_cmd(key, opcode, opaque, server_state) do
    # Lager.info "get key: #{key}"
    case MemcachedE.get(key) do
      {value, flags, cas} ->
        len = size(value)
        if len > 0 do
          response = make_response_header(server_state, opcode, 0, 4, 0, @protocol_binray_response_success, len + 4, opaque, cas)
            <> << flags::size(32) >> <> value
        else
          response = make_response_header(server_state, opcode, 0, 4, 0, @protocol_binray_response_success, 4, opaque, cas)
            <> << flags::size(32) >>
        end
        send_data(server_state, response)
      :not_found -> :ok # send_response_header(server_state, opcode, 0, 0, 0, @protocol_binray_response_key_enoent, 0, opaque)
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
          response = make_response_header(server_state, opcode, keylen, 4, 0, @protocol_binray_response_success, len + keylen + extlen, opaque, cas)
            <> << flags::size(32) >> <> key <> value
        else
          response = make_response_header(server_state, opcode, keylen, 4, 0, @protocol_binray_response_success, keylen + extlen, opaque, cas)
            <> << flags::size(32) >> <> key
        end
        send_data(server_state, response)
      :not_found -> :ok # send_response_header(server_state, opcode, 0, 0, 0, @protocol_binray_response_key_enoent, 0, opaque)
    end
    server_state
  end

  def read_remainder(data, expected, server_state) do
    cond do
      size(data) == expected -> data
      size(data) > expected ->
        <<data::[binary, size(expected)], rest::binary >> = data
        {data, rest}
      true ->
        case server_state.transport.recv(server_state.socket, 0, 5000) do
          {:ok, read} ->
            data = data <> read
            read_remainder(data <> read, expected, server_state)
          res ->
            Lager.info "read_remainder res: #{inspect res}"
            :ok = close_transport(server_state)
            nil
        end
    end
  end

  defp key_data(extlen, keylen, buffer) do
    case extlen do
      0 -> << key::[binary, size(keylen)], data::binary >> = buffer
      _ ->
        extlen = extlen * 8
        << _::size(extlen), key::[binary, size(keylen)], data::binary >> = buffer
    end
    {key, data}
  end

  defp send_data(server_state, data), do: server_state.transport.send(server_state.socket, data)

  defp send_saved_responses(server_state) do
    Enum.each(server_state.saved_responses, fn (response) ->
      send_data(server_state, response)
    end)
    server_state.saved_responses []
  end

  defp close_transport(server_state), do: server_state.transport.close(server_state.socket)

end
