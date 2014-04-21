defmodule MemcachedE.Server do
  use GenServer
  require Lager
  require MemcachedE.BaseDefinitions
  alias MemcachedE.BaseDefinitions, as: Bd
  alias MemcachedE.BinaryCommands, as: B

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
        if String.starts_with?(data, << Bd.protocol_binary_req >>) do
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
      Bd.protocol_binray_cmd_noop -> B.send_response_header(server_state, opcode, 0, 0, 0, Bd.protocol_binray_response_success, 0, opaque)
      Bd.protocol_binray_cmd_quit ->
        B.send_response_header(server_state, opcode, 0, 0, 0, Bd.protocol_binray_response_success, 0, opaque)
        close_transport(server_state)
      Bd.protocol_binray_cmd_quitq ->
        close_transport(server_state)
      Bd.protocol_binray_cmd_set ->
        {key, data} = key_data(extlen, keylen, tail)
        data = read_remainder(data, bodylen - extlen - keylen, server_state)
        case read_remainder(data, bodylen - extlen - keylen, server_state) do
          {data, rest} ->
            server_state = B.binary_set_cmd(key, data, opcode, opaque, server_state)
            server_state = handle_binary_protocol(rest, server_state)
          data -> server_state = B.binary_set_cmd(key, data, opcode, opaque, server_state)
        end
      Bd.protocol_binray_cmd_setq ->
        {key, data} = key_data(extlen, keylen, tail)
        case read_remainder(data, bodylen - extlen - keylen, server_state) do
          {data, rest} ->
            MemcachedE.set(key, data, 0, 0)
            server_state = handle_binary_protocol(rest, server_state)
          data -> MemcachedE.set(key, data, 0, 0)
        end
      Bd.protocol_binray_cmd_add ->
        {key, data} = key_data(extlen, keylen, tail)
        case read_remainder(data, bodylen - extlen - keylen, server_state) do
          {data, rest} ->
            server_state = B.binary_add_cmd(key, data, opcode, opaque, server_state)
            server_state = handle_binary_protocol(rest, server_state)
          data -> B.binary_add_cmd(key, data, opcode, opaque, server_state)
        end
      Bd.protocol_binray_cmd_addq ->
        {key, data} = key_data(extlen, keylen, tail)
        case read_remainder(data, bodylen - extlen - keylen, server_state) do
          {data, rest} ->
            server_state = B.binary_addq_cmd(key, data, opcode, opaque, server_state)
            server_state = handle_binary_protocol(rest, server_state)
          data -> server_state = B.binary_addq_cmd(key, data, opcode, opaque, server_state)
        end
      Bd.protocol_binray_cmd_replace ->
        {key, data} = key_data(extlen, keylen, tail)
        case read_remainder(data, bodylen - extlen - keylen, server_state) do
          {data, rest} ->
            server_state = B.binary_replace_cmd(key, data, opcode, opaque, server_state)
            server_state = handle_binary_protocol(rest, server_state)
          data -> server_state = B.binary_replace_cmd(key, data, opcode, opaque, server_state)
        end
      Bd.protocol_binray_cmd_replaceq ->
        {key, data} = key_data(extlen, keylen, tail)
        case read_remainder(data, bodylen - extlen - keylen, server_state) do
          {data, rest} ->
            server_state = B.binary_replaceq_cmd(key, data, opcode, opaque, server_state)
            server_state = handle_binary_protocol(rest, server_state)
          data -> server_state = B.binary_replaceq_cmd(key, data, opcode, opaque, server_state)
        end
      Bd.protocol_binray_cmd_delete ->
        {key, data} = key_data(extlen, keylen, tail)
        case read_remainder(data, bodylen - extlen - keylen, server_state) do
          {_, rest} ->
            server_state = B.binary_delete_cmd(key, opcode, opaque, server_state)
            server_state = handle_binary_protocol(rest, server_state)
          _ -> server_state = B.binary_delete_cmd(key, opcode, opaque, server_state)
        end
      Bd.protocol_binray_cmd_deleteq ->
        {key, data} = key_data(extlen, keylen, tail)
        server_state = B.binary_deleteq_cmd(key, opcode, opaque, server_state)
      Bd.protocol_binray_cmd_get ->
        {key, data} = key_data(extlen, keylen, tail)
        server_state = B.binary_get_cmd(key, opcode, opaque, server_state)
        server_state = handle_binary_protocol(data, server_state)
      Bd.protocol_binray_cmd_getq ->
        {key, data} = key_data(extlen, keylen, tail)
        server_state = B.binary_getq_cmd(key, opcode, opaque, server_state)
        server_state = handle_binary_protocol(data, server_state)
      Bd.protocol_binray_cmd_getk ->
        {key, data} = key_data(extlen, keylen, tail)
        server_state = B.binary_getk_cmd(key, opcode, opaque, server_state)
        server_state = handle_binary_protocol(data, server_state)
      Bd.protocol_binray_cmd_getkq ->
        {key, data} = key_data(extlen, keylen, tail)
        server_state = B.binary_getkq_cmd(key, opcode, opaque, server_state)
        server_state = handle_binary_protocol(data, server_state)
      Bd.protocol_binray_cmd_increment ->
        << count::size(64), intial::size(64), expiration::[unsigned, integer, size(32)], key::[binary, size(keylen)], data::binary >> = tail
        server_state = B.binary_incr_cmd(key, count, intial, expiration, opcode, opaque, server_state)
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
          Bd.send_data(server_state, <<"END\r\n">>)
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
    Bd.send_data(server_state, <<"ERROR\r\n">>)
  end

  defp set_cmd(loop_state = LoopState[no_reply: nil], value, server_state) do
    case MemcachedE.set(loop_state.key, value, loop_state.flags, loop_state.exptime) do
      {:stored, _} -> Bd.send_data(server_state, <<"STORED\r\n">>)
      :not_stored -> Bd.send_data(server_state, <<"NOT_STORED\r\n">>)
      :exists -> Bd.send_data(server_state, <<"EXISTS\r\n">>)
      :not_found -> Bd.send_data(server_state, <<"NOT_FOUND\r\n">>)
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
        Bd.send_data(server_state, data)
        Bd.send_data(server_state, value <> <<"\r\n">>)
    end
    get_cmd tail, server_state
  end

  defp get_cmd([], server_state) do
    Bd.send_data(server_state, <<"END\r\n">>)
  end

  defp delete_cmd([key], server_state) do
    case MemcachedE.delete(key) do
      :deleted -> Bd.send_data(server_state, <<"DELETED\r\n">>)
      :not_found -> Bd.send_data(server_state, <<"NOT_FOUND\r\n">>)
      true -> send_error(server_state)
    end
  end

  defp delete_cmd([key, _], _server_state) do
    MemcachedE.delete(key)
  end

  defp incr_cmd([key], server_state) do
    case MemcachedE.incr(key, 1) do
      :not_found -> Bd.send_data(server_state, <<"NOT_FOUND\r\n">>)
      value when is_binary(value) -> Bd.send_data(server_state, value <> <<"\r\n">>)
      true -> send_error(server_state)
    end
  end

  defp incr_cmd([key, count], server_state) do
    case MemcachedE.incr(key, binary_to_integer(count)) do
      :not_found -> Bd.send_data(server_state, <<"NOT_FOUND\r\n">>)
      value when is_binary(value) -> Bd.send_data(server_state, value <> <<"\r\n">>)
      true -> send_error(server_state)
    end
  end

  defp incr_cmd([key, count, _], _server_state) do
    MemcachedE.incr(key, binary_to_integer(count))
  end

  defp decr_cmd([key], server_state) do
    case MemcachedE.decr(key, 1) do
      :not_found -> Bd.send_data(server_state, <<"NOT_FOUND\r\n">>)
      value when is_binary(value) -> Bd.send_data(server_state, value <> <<"\r\n">>)
      true -> send_error(server_state)
    end
  end

  defp decr_cmd([key, count], server_state) do
    case MemcachedE.decr(key, binary_to_integer(count)) do
      :not_found -> Bd.send_data(server_state, <<"NOT_FOUND\r\n">>)
      value when is_binary(value) -> Bd.send_data(server_state, value <> <<"\r\n">>)
      true -> send_error(server_state)
    end
  end

  defp decr_cmd([key, count, _], _server_state) do
    MemcachedE.incr(key, binary_to_integer(count))
  end

  defp stats_cmd(["stats","cachedump","1", "0", "0"], server_state) do
    Bd.send_data(server_state, <<"END\r\n">>)
  end

  defp stats_cmd(["stats","cachedump","200", "0", "0"], server_state) do
    Bd.send_data(server_state, <<"CLIENT_ERROR\r\n">>)
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

  defp close_transport(server_state), do: server_state.transport.close(server_state.socket)

end
