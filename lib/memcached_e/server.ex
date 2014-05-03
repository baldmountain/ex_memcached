defmodule MemcachedE.Server do
  use GenServer
  require Lager
  require MemcachedE.BaseDefinitions
  alias MemcachedE.BaseDefinitions, as: Bd
  alias MemcachedE.BinaryCommands, as: B

  @receive_timeout 5000

  defrecord ServerState, socket: nil, transport: nil, existing_data: "", stored_responses: << >>

  def start_link(ref, socket, transport, opts) do
    pid = spawn_link(__MODULE__, :init, [ref, socket, transport, opts])
    {:ok, pid}
  end

  def init(ref, socket, transport, _opts \\ []) do
    :ok = :ranch.accept_ack(ref)
    loop(ServerState[socket: socket, transport: transport])
  end

  defp loop(server_state) do
  	case server_state.transport.recv(server_state.socket, 0, @receive_timeout) do
  		{:ok, data} ->
        data = server_state.existing_data <> data
        server_state = server_state.existing_data ""
        if String.starts_with?(data, << Bd.protocol_binary_req >>) do
          server_state = handle_binary_protocol(server_state, data)
        else
          if String.contains?(data, "\r\n") do
            server_state = handle_ascii_protocol(server_state, data)
          else
            server_state = server_state.existing_data(server_state.existing_data <> data)
          end
        end
  			loop(server_state)
  		res ->
        Lager.info "loop res: #{inspect res}"
  			:ok = close_transport(server_state)
  	end
  end

  defp handle_binary_protocol(server_state, <<>>) do
    server_state
  end

  defp handle_binary_protocol(server_state, data) do
    # server_state = try do
      data = read_expected server_state, data, 24
      << Bd.protocol_binary_req, opcode, keylen::[big, unsigned, integer, size(16)], extlen, _datatype, _reserved::[big, unsigned, integer, size(16)],
        bodylen::[big, unsigned, integer, size(32)], opaque::[big, unsigned, integer, size(32)], cas::[big, unsigned, integer, size(64)], tail::binary >> = data
        # Lager.info "magic #{magic} #{Bd.opcode_description opcode} keylen #{keylen} extlen #{extlen} datatype #{datatype} reserved #{reserved} bodylen #{bodylen} opaque #{opaque} cas #{cas}"

      if keylen > 250 do
        B.send_error(server_state, opcode, opaque, Bd.protocol_binray_response_e2big)
        close_transport(server_state)
      else
        case opcode do
          Bd.protocol_binray_cmd_noop ->
            server_state = B.send_stored_responses server_state
            server_state = B.send_response_header(server_state, opcode, 0, 0, 0, Bd.protocol_binray_response_success, 0, opaque)
          Bd.protocol_binray_cmd_quit ->
            server_state = B.send_stored_responses
            server_state = B.send_response_header(server_state, opcode, 0, 0, 0, Bd.protocol_binray_response_success, 0, opaque)
            close_transport(server_state)
          Bd.protocol_binray_cmd_quitq ->
            close_transport(server_state)
          Bd.protocol_binray_cmd_set when extlen == 8 ->
            tail = read_expected server_state, tail, extlen + keylen
            data_len = bodylen - extlen - keylen
            << flags::size(32), exptime::size(32), key::[binary, size(keylen)], data::binary >> = tail
            cond do
              data_len > Application.get_env(:memcached_e, :max_data_size) ->
                # delete if the key exists
                MemcachedE.delete key
                case read_remainder(server_state, data, bodylen - extlen - keylen) do
                  {_data, rest} ->
                    server_state = B.send_error(server_state, opcode, opaque, Bd.protocol_binray_response_e2big)
                    server_state = handle_binary_protocol(server_state, rest)
                  _data -> B.send_error(server_state, opcode, opaque, Bd.protocol_binray_response_e2big)
                end
              true ->
                case read_remainder(server_state, data, bodylen - extlen - keylen) do
                  {data, rest} ->
                    server_state = B.binary_set_cmd(key, data, flags, exptime, opcode, opaque, cas, server_state)
                    server_state = handle_binary_protocol(server_state, rest)
                  data -> server_state = B.binary_set_cmd(key, data, flags, exptime, opcode, opaque, cas, server_state)
                end
            end
          Bd.protocol_binray_cmd_setq when extlen == 8 ->
            tail = read_expected server_state, tail, extlen + keylen
            data_len = bodylen - extlen - keylen
            << flags::size(32), exptime::size(32), key::[binary, size(keylen)], data::binary >> = tail
            cond do
              data_len > Application.get_env(:memcached_e, :max_data_size) ->
                # delete if the key exists
                MemcachedE.delete key
                case read_remainder(server_state, data, bodylen - extlen - keylen) do
                  {_data, rest} ->
                    server_state = B.send_error(server_state, opcode, opaque, Bd.protocol_binray_response_e2big)
                    server_state = handle_binary_protocol(server_state, rest)
                  _data -> B.send_error(server_state, opcode, opaque, Bd.protocol_binray_response_e2big)
                end
              true ->
                case read_remainder(server_state, data, bodylen - extlen - keylen) do
                  {data, rest} ->
                    MemcachedE.set(key, data, flags, exptime, cas)
                    server_state = handle_binary_protocol(server_state, rest)
                  data -> MemcachedE.set(key, data, flags, exptime, cas)
                end
            end
          Bd.protocol_binray_cmd_add ->
            tail = read_expected server_state, tail, extlen + keylen
            data_len = bodylen - extlen - keylen
            << flags::size(32), exptime::size(32), key::[binary, size(keylen)], data::binary >> = tail
            cond do
              data_len > Application.get_env(:memcached_e, :max_data_size) ->
                # delete if the key exists
                MemcachedE.delete key
                server_state = case read_remainder(server_state, data, bodylen - extlen - keylen) do
                  {_data, rest} ->
                    server_state = B.send_stored_responses server_state
                    server_state = B.send_error(server_state, opcode, opaque, Bd.protocol_binray_response_e2big)
                    server_state = handle_binary_protocol(server_state, rest)
                  _data -> B.send_error(server_state, opcode, opaque, Bd.protocol_binray_response_e2big)
                end
              true ->
                server_state = case read_remainder(server_state, data, bodylen - extlen - keylen) do
                  {data, rest} ->
                    server_state = B.send_stored_responses server_state
                    server_state = B.binary_add_cmd(key, data, flags, exptime, opcode, opaque, server_state)
                    server_state = handle_binary_protocol(server_state, rest)
                  data -> B.binary_add_cmd(key, data, flags, exptime, opcode, opaque, server_state)
                end
            end
          Bd.protocol_binray_cmd_addq ->
            tail = read_expected server_state, tail, extlen + keylen
            data_len = bodylen - extlen - keylen
            << flags::size(32), exptime::size(32), key::[binary, size(keylen)], data::binary >> = tail
            cond do
              data_len > Application.get_env(:memcached_e, :max_data_size) ->
                # delete if the key exists
                MemcachedE.delete key
                case read_remainder(server_state, data, bodylen - extlen - keylen) do
                  {_data, rest} ->
                    B.send_error(server_state, opcode, opaque, Bd.protocol_binray_response_e2big)
                    server_state = handle_binary_protocol(server_state, rest)
                  _data -> B.send_error(server_state, opcode, opaque, Bd.protocol_binray_response_e2big)
                end
              true ->
                case read_remainder(server_state, data, bodylen - extlen - keylen) do
                  {data, rest} ->
                    server_state = B.binary_addq_cmd(key, data, flags, exptime, opcode, opaque, server_state)
                    server_state = handle_binary_protocol(server_state, rest)
                  data -> server_state = B.binary_addq_cmd(key, data, flags, exptime, opcode, opaque, server_state)
                end
            end
          Bd.protocol_binray_cmd_replace ->
            tail = read_expected server_state, tail, extlen + keylen
            data_len = bodylen - extlen - keylen
            << flags::size(32), exptime::size(32), key::[binary, size(keylen)], data::binary >> = tail
            cond do
              data_len > Application.get_env(:memcached_e, :max_data_size) ->
                # delete if the key exists
                MemcachedE.delete key
                case read_remainder(server_state, data, bodylen - extlen - keylen) do
                  {_data, rest} ->
                    B.send_error(server_state, opcode, opaque, Bd.protocol_binray_response_e2big)
                    server_state = handle_binary_protocol(server_state, rest)
                  _data -> B.send_error(server_state, opcode, opaque, Bd.protocol_binray_response_e2big)
                end
              true ->
                case read_remainder(server_state, data, bodylen - extlen - keylen) do
                  {data, rest} ->
                    server_state = B.binary_replace_cmd(key, data, flags, exptime, opcode, opaque, server_state)
                    server_state = handle_binary_protocol(server_state, rest)
                  data -> server_state = B.binary_replace_cmd(key, data, flags, exptime, opcode, opaque, server_state)
                end
            end
          Bd.protocol_binray_cmd_replaceq ->
            tail = read_expected server_state, tail, extlen + keylen
            data_len = bodylen - extlen - keylen
            << flags::size(32), exptime::size(32), key::[binary, size(keylen)], data::binary >> = tail
            cond do
              data_len > Application.get_env(:memcached_e, :max_data_size) ->
                # delete if the key exists
                MemcachedE.delete key
                case read_remainder(server_state, data, bodylen - extlen - keylen) do
                  {_data, rest} ->
                    B.send_error(server_state, opcode, opaque, Bd.protocol_binray_response_e2big)
                    server_state = handle_binary_protocol(server_state, rest)
                  _data -> B.send_error(server_state, opcode, opaque, Bd.protocol_binray_response_e2big)
                end
              true ->
                case read_remainder(server_state, data, bodylen - extlen - keylen) do
                  {data, rest} ->
                    server_state = B.binary_replaceq_cmd(key, data, flags, exptime, opcode, opaque, server_state)
                    server_state = handle_binary_protocol(server_state, rest)
                  data -> server_state = B.binary_replaceq_cmd(key, data, flags, exptime, opcode, opaque, server_state)
                end
            end
          Bd.protocol_binray_cmd_delete ->
            {key, data} = key_data(extlen, keylen, tail)
            case read_remainder(server_state, data, bodylen - extlen - keylen) do
              {_, rest} ->
                server_state = B.binary_delete_cmd(key, opcode, opaque, server_state)
                server_state = handle_binary_protocol(server_state, rest)
              _ -> server_state = B.binary_delete_cmd(key, opcode, opaque, server_state)
            end
          Bd.protocol_binray_cmd_deleteq ->
            {key, data} = key_data(extlen, keylen, tail)
            server_state = B.binary_deleteq_cmd(key, opcode, opaque, server_state)
            server_state = handle_binary_protocol(server_state, data)
          Bd.protocol_binray_cmd_get ->
            {key, data} = key_data(extlen, keylen, tail)
            server_state = B.binary_get_cmd(key, opcode, opaque, server_state)
            server_state = handle_binary_protocol(server_state, data)
          Bd.protocol_binray_cmd_getq ->
            {key, data} = key_data(extlen, keylen, tail)
            server_state = B.binary_getq_cmd(key, opcode, opaque, server_state)
            server_state = handle_binary_protocol(server_state, data)
          Bd.protocol_binray_cmd_getk ->
            {key, data} = key_data(extlen, keylen, tail)
            server_state = B.binary_getk_cmd(key, opcode, opaque, server_state)
            server_state = handle_binary_protocol(server_state, data)
          Bd.protocol_binray_cmd_getkq ->
            {key, data} = key_data(extlen, keylen, tail)
            server_state = B.binary_getkq_cmd(key, opcode, opaque, server_state)
            server_state = handle_binary_protocol(server_state, data)
          Bd.protocol_binray_cmd_increment ->
            tail = read_expected server_state, tail, extlen + keylen
            << count::[unsigned, size(64)], intial::size(64), expiration::[unsigned, integer, size(32)], key::[binary, size(keylen)], data::binary >> = tail
            server_state = B.binary_incr_cmd(key, count, intial, expiration, opcode, opaque, server_state)
            server_state = handle_binary_protocol(server_state, data)
          Bd.protocol_binray_cmd_incrementq ->
            tail = read_expected server_state, tail, extlen + keylen
            << count::size(64), intial::size(64), expiration::[unsigned, integer, size(32)], key::[binary, size(keylen)], data::binary >> = tail
            server_state = B.binary_incrq_cmd(key, count, intial, expiration, opcode, opaque, server_state)
            server_state = handle_binary_protocol(server_state, data)
          Bd.protocol_binray_cmd_decrement ->
            tail = read_expected server_state, tail, extlen + keylen
            << count::size(64), intial::size(64), expiration::[unsigned, integer, size(32)], key::[binary, size(keylen)], data::binary >> = tail
            server_state = B.binary_decr_cmd(key, count, intial, expiration, opcode, opaque, server_state)
            server_state = handle_binary_protocol(server_state, data)
          Bd.protocol_binray_cmd_decrementq ->
            tail = read_expected server_state, tail, extlen + keylen
            << count::size(64), intial::size(64), expiration::[unsigned, integer, size(32)], key::[binary, size(keylen)], data::binary >> = tail
            server_state = B.binary_decrq_cmd(key, count, intial, expiration, opcode, opaque, server_state)
            server_state = handle_binary_protocol(server_state, data)
          Bd.protocol_binray_cmd_version ->
            {_, data} = key_data(extlen, keylen, tail)
            server_state = B.binary_version_cmd(opcode, opaque, server_state)
            server_state = handle_binary_protocol(server_state, data)
          Bd.protocol_binray_cmd_flush ->
            case extlen do
              0 ->
                expiration = 0
                data = tail
              _ ->
                len = 8 * extlen
                tail = read_expected server_state, tail, len
                << expiration::size(len), data::binary >> = tail
            end
            server_state = B.binary_flush_cmd(expiration, opcode, opaque, server_state)
            server_state = handle_binary_protocol(server_state, data)
          Bd.protocol_binray_cmd_flushq ->
            case extlen do
              0 ->
                expiration = 0
                data = tail
              _ ->
                len = 8 * extlen
                tail = read_expected server_state, tail, len
                << expiration::size(len), data::binary >> = tail
            end
            server_state = B.binary_flushq_cmd(expiration, opcode, opaque, server_state)
            server_state = handle_binary_protocol(server_state, data)
          Bd.protocol_binray_cmd_append ->
            {key, data} = key_data(extlen, keylen, tail)
            case read_remainder(server_state, data, bodylen - extlen - keylen) do
              {data, rest} ->
                server_state = B.binary_append_cmd(key, data, opcode, opaque, server_state)
                server_state = handle_binary_protocol(server_state, rest)
              data -> server_state = B.binary_append_cmd(key, data, opcode, opaque, server_state)
            end
          Bd.protocol_binray_cmd_appendq ->
            {key, data} = key_data(extlen, keylen, tail)
            case read_remainder(server_state, data, bodylen - extlen - keylen) do
              {data, rest} ->
                server_state = B.binary_appendq_cmd(key, data, opcode, opaque, server_state)
                server_state = handle_binary_protocol(server_state, rest)
              data -> server_state = B.binary_appendq_cmd(key, data, opcode, opaque, server_state)
            end
          Bd.protocol_binray_cmd_prepend ->
            {key, data} = key_data(extlen, keylen, tail)
            case read_remainder(server_state, data, bodylen - extlen - keylen) do
              {data, rest} ->
                server_state = B.binary_prepend_cmd(key, data, opcode, opaque, server_state)
                server_state = handle_binary_protocol(server_state, rest)
              data -> server_state = B.binary_prepend_cmd(key, data, opcode, opaque, server_state)
            end
          Bd.protocol_binray_cmd_prependq ->
            {key, data} = key_data(extlen, keylen, tail)
            case read_remainder(server_state, data, bodylen - extlen - keylen) do
              {data, rest} ->
                server_state = B.binary_prependq_cmd(key, data, opcode, opaque, server_state)
                server_state = handle_binary_protocol(server_state, rest)
              data -> server_state = B.binary_prependq_cmd(key, data, opcode, opaque, server_state)
            end
          Bd.protocol_binray_cmd_touch ->
            tail = read_expected server_state, tail, extlen + keylen
            << exptime::size(32), key::[binary, size(keylen)], data::binary >> = tail
            server_state = B.binary_touch_cmd(key, exptime, opcode, opaque, server_state)
            server_state = handle_binary_protocol(server_state, data)
          Bd.protocol_binray_cmd_gat ->
            tail = read_expected server_state, tail, extlen + keylen
            << exptime::size(32), key::[binary, size(keylen)], data::binary >> = tail
            server_state = B.binary_gat_cmd(key, exptime, opcode, opaque, server_state)
            server_state = handle_binary_protocol(server_state, data)
          Bd.protocol_binray_cmd_stat ->
            {key, data} = key_data(extlen, keylen, tail)
            case key do
              "settings" ->
                send_stat_response server_state, "maxconns", <<"#{Application.get_env(:memcached_e, :max_connections)}">>, opcode, opaque
                send_stat_response server_state, "domain_socket", "NULL", opcode, opaque
                send_stat_response server_state, "evictions", "on", opcode, opaque
                send_stat_response server_state, "cas_enabled", "yes", opcode, opaque
                send_stat_response server_state, "flush_enabled", "yes", opcode, opaque
                B.send_response_header(server_state, opcode, 0, 0, 0, Bd.protocol_binray_response_success, 0, opaque)
              _ ->
                B.send_response_header(server_state, opcode, 0, 0, 0, Bd.protocol_binray_response_success, 0, opaque)
              end
            server_state = handle_binary_protocol(server_state, data)
          _ ->
            Lager.info "Unknown opcode: #{opcode}"
            {_key, data} = key_data(extlen, keylen, tail)
            B.send_response_header(server_state, opcode, 0, 0, 0, Bd.protocol_binray_response_unknown_command, 0, opaque)
            server_state = handle_binary_protocol(server_state, data)
        end
      end
      server_state
    # catch
    #   :error, :badmatch ->
    #     Lager.info "Badmatch on input command"
    #     << _magic, opcode, _tail::binary >> = data
    #     B.send_response_header(server_state, opcode, 0, 0, 0, Bd.protocol_binray_response_einval, 0, 0)
    #   :exit, value ->
    #     Lager.info "exit called with #{inspect value}"
    #     server_state
    #   :throw, value ->
    #     Lager.info "Throw called with #{inspect value}"
    #     server_state
    #   what, value ->
    #     Lager.info "1> Caught #{what} with #{inspect value}"
    #     try do
    #       << _magic, opcode, _keylen::[big, unsigned, integer, size(16)], _extlen, _datatype, _reserved::[big, unsigned, integer, size(16)],
    #         _bodylen::[big, unsigned, integer, size(32)], opaque::[big, unsigned, integer, size(32)], _tail::binary >> = data
    #       B.send_response_header(server_state, opcode, 0, 0, 0, Bd.protocol_binray_response_einval, 0, opaque)
    #       server_state
    #     catch
    #       what, value ->
    #         Lager.info "2> Caught secondary #{inspect what} with #{inspect value}"
    #         server_state
    #     end
    # end
    server_state
  end

  defrecord LoopState, state: :commands, key: nil, flags: 0, exptime: 0, cas: 0, data_length: 0, no_reply: nil

  defp handle_ascii_protocol(server_state, data) do
    cmds = String.split data, "\r\n"
    if size(List.last(cmds)) == 0, do: cmds = List.delete_at(cmds, -1)
    # Lager.info "cmds: #{inspect cmds}"

    {server_state, _} = Enum.reduce cmds, {server_state, LoopState.new}, fn(cmd, {server_state, loop_state}) ->
      command = if (loop_state.state == :commands) do
        parts = String.split cmd, " "
        List.first(parts)
      end
      # for gurds
      cmd_size = size(cmd)
      data_length = loop_state.data_length
      case {loop_state.state, command} do
        {:commands, "get"} ->
          [_|tail] = parts
          get_cmd(tail, server_state)
          loop_state = LoopState.new
        {:commands, "gets"} ->
          [_|tail] = parts
          gets_cmd(tail, server_state)
          loop_state = LoopState.new
        {:commands, "set"} ->
          # Lager.info "set: #{inspect parts}"
          case parts do
            [_, k, flags, exptime, data_length] ->
              loop_state = try do
                LoopState[state: :set, key: k, flags: binary_to_integer(flags), exptime: binary_to_integer(exptime), data_length: binary_to_integer(data_length)]
              catch
                :error, :badarg ->
                  send_ascii_reply :bad_command_line, server_state
                  LoopState.new
                what, value ->
                  Lager.info "3> Caught #{inspect what} with #{inspect value}"
                  LoopState.new
              end
            [_, k, flags, exptime, data_length, nr] ->
              loop_state = try do
                LoopState[state: :set, key: k, flags: binary_to_integer(flags), exptime: binary_to_integer(exptime), data_length: binary_to_integer(data_length), no_reply: nr]
              catch
                :error, :badarg ->
                  send_ascii_reply :bad_command_line, server_state
                  LoopState.new
                what, value ->
                  Lager.info "4> Caught #{inspect what} with #{inspect value}"
                  LoopState.new
              end
            _ ->
              send_error(server_state)
              loop_state = LoopState.new
          end
        {:set, _} when cmd_size == data_length ->
          cond do
            data_length <= Application.get_env(:memcached_e, :max_data_size) ->
              set_cmd(loop_state, cmd, server_state)
            true ->
              Bd.send_data(server_state, <<"SERVER_ERROR object too large for cache\r\n">>)
          end
          loop_state = LoopState.new
        {:set, _} when cmd_size < data_length ->
          case read_remainder_ascii(server_state, cmd, data_length) do
            {cmd, rest} ->
              cond do
                data_length <= Application.get_env(:memcached_e, :max_data_size) ->
                  set_cmd(loop_state, cmd, server_state)
                true ->
                  Bd.send_data(server_state, <<"SERVER_ERROR object too large for cache\r\n">>)
              end
              loop_state = LoopState.new
              server_state = cond do
                size(rest) > 0 ->
                  # Lager.info "Adding '#{rest}' to existing_data"
                  server_state.existing_data(rest)
                true -> server_state
              end
            _ ->
              # Lager.info "set error:"
              loop_state = LoopState.new
              :ok
          end
        {:set, _} ->
          Lager.info "bad data length"
          Bd.send_data(server_state, << "SERVER_ERROR object too large for cache\r\n" >>)
          loop_state = LoopState.new
        {:commands, "add"} ->
          # Lager.info "add: #{inspect parts}"
          case parts do
            [_, k, flags, exptime, data_length] ->
              loop_state = try do
                LoopState[state: :add, key: k, flags: binary_to_integer(flags), exptime: binary_to_integer(exptime), data_length: binary_to_integer(data_length)]
              catch
                :error, :badarg ->
                  send_ascii_reply :bad_command_line, server_state
                  LoopState.new
                what, value ->
                  Lager.info "5> Caught #{inspect what} with #{inspect value}"
                  LoopState.new
              end
            [_, k, flags, exptime, data_length, nr] ->
              loop_state = try do
                LoopState[state: :add, key: k, flags: binary_to_integer(flags), exptime: binary_to_integer(exptime), data_length: binary_to_integer(data_length), no_reply: nr]
              catch
                :error, :badarg ->
                  send_ascii_reply :bad_command_line, server_state
                  LoopState.new
                what, value ->
                  Lager.info "6> Caught #{inspect what} with #{inspect value}"
                  LoopState.new
              end
            _ ->
              send_error(server_state)
              loop_state = LoopState.new
          end
        {:add, _} when cmd_size == data_length ->
          add_cmd(loop_state, cmd, server_state)
          loop_state = LoopState.new
        {:add, _} when cmd_size < data_length ->
          case read_remainder_ascii(server_state, cmd, data_length) do
            {cmd, rest} ->
              add_cmd(loop_state, cmd, server_state)
              loop_state = LoopState.new
              server_state = cond do
                size(rest) > 0 -> server_state.existing_data(rest)
                true -> server_state
              end
            _ ->
              loop_state = LoopState.new
              :ok
          end
        {:add, _} ->
          Lager.info "bad data length"
          send_error(server_state)
          loop_state = LoopState.new
        {:commands, "replace"} ->
          # Lager.info "replace: #{inspect parts}"
          case parts do
            [_, k, flags, exptime, data_length] ->
              loop_state = try do
                LoopState[state: :replace, key: k, flags: binary_to_integer(flags), exptime: binary_to_integer(exptime), data_length: binary_to_integer(data_length)]
              catch
                :error, :badarg ->
                  send_ascii_reply :bad_command_line, server_state
                  LoopState.new
                what, value ->
                  Lager.info "7> Caught #{inspect what} with #{inspect value}"
                  LoopState.new
              end
            [_, k, flags, exptime, data_length, nr] ->
              loop_state = try do
                LoopState[state: :replace, key: k, flags: binary_to_integer(flags), exptime: binary_to_integer(exptime), data_length: binary_to_integer(data_length), no_reply: nr]
              catch
                :error, :badarg ->
                  send_ascii_reply :bad_command_line, server_state
                  LoopState.new
                what, value ->
                  Lager.info "8> Caught #{inspect what} with #{inspect value}"
                  LoopState.new
              end
            _ ->
              send_error(server_state)
              loop_state = LoopState.new
          end
        {:replace, _} when cmd_size == data_length ->
          replace_cmd(loop_state, cmd, server_state)
          loop_state = LoopState.new
        {:replace, _} when cmd_size < data_length ->
          case read_remainder_ascii(server_state, cmd, data_length) do
            {cmd, rest} ->
              replace_cmd(loop_state, cmd, server_state)
              loop_state = LoopState.new
              server_state = cond do
                size(rest) > 0 -> server_state.existing_data(rest)
                true -> server_state
              end
            _ ->
              loop_state = LoopState.new
              :ok
          end
        {:replace, _} ->
          Lager.info "bad data length"
          send_error(server_state)
          loop_state = LoopState.new
        {:commands, "append"} ->
          # Lager.info "append: #{inspect parts}"
          case parts do
            [_, k, flags, exptime, data_length] ->
              loop_state = try do
                LoopState[state: :append, key: k, flags: binary_to_integer(flags), exptime: binary_to_integer(exptime), data_length: binary_to_integer(data_length)]
              catch
                :error, :badarg ->
                  send_ascii_reply :bad_command_line, server_state
                  LoopState.new
                what, value ->
                  Lager.info "9> Caught #{inspect what} with #{inspect value}"
                  LoopState.new
              end
            [_, k, flags, exptime, data_length, nr] ->
              loop_state = try do
                LoopState[state: :append, key: k, flags: binary_to_integer(flags), exptime: binary_to_integer(exptime), data_length: binary_to_integer(data_length), no_reply: nr]
              catch
                :error, :badarg ->
                  send_ascii_reply :bad_command_line, server_state
                  LoopState.new
                what, value ->
                  Lager.info "10> Caught #{inspect what} with #{inspect value}"
                  LoopState.new
              end
            _ ->
              send_error(server_state)
              loop_state = LoopState.new
          end
        {:append, _} when cmd_size == data_length ->
          append_cmd(loop_state, cmd, server_state)
          loop_state = LoopState.new
        {:append, _} when cmd_size < data_length ->
          case read_remainder_ascii(server_state, cmd, data_length) do
            {cmd, rest} ->
              append_cmd(loop_state, cmd, server_state)
              loop_state = LoopState.new
              server_state = cond do
                size(rest) > 0 -> server_state.existing_data(rest)
                true -> server_state
              end
            _ ->
              loop_state = LoopState.new
              :ok
          end
        {:append, _} ->
          Lager.info "bad data length"
          send_error(server_state)
          loop_state = LoopState.new
        {:commands, "prepend"} ->
          # Lager.info "prepend: #{inspect parts}"
          case parts do
            [_, k, flags, exptime, data_length] ->
              loop_state = try do
                LoopState[state: :prepend, key: k, flags: binary_to_integer(flags), exptime: binary_to_integer(exptime), data_length: binary_to_integer(data_length)]
              catch
                :error, :badarg ->
                  send_ascii_reply :bad_command_line, server_state
                  LoopState.new
                what, value ->
                  Lager.info "11> Caught #{inspect what} with #{inspect value}"
                  LoopState.new
              end
            [_, k, flags, exptime, data_length, nr] ->
              loop_state = try do
                LoopState[state: :prepend, key: k, flags: binary_to_integer(flags), exptime: binary_to_integer(exptime), data_length: binary_to_integer(data_length), no_reply: nr]
              catch
                :error, :badarg ->
                  send_ascii_reply :bad_command_line, server_state
                  LoopState.new
                what, value ->
                  Lager.info "12> Caught #{inspect what} with #{inspect value}"
                  LoopState.new
              end
            _ ->
              send_error(server_state)
              loop_state = LoopState.new
          end
        {:prepend, _} when cmd_size == data_length ->
          prepend_cmd(loop_state, cmd, server_state)
          loop_state = LoopState.new
        {:prepend, _} when cmd_size < data_length ->
          case read_remainder_ascii(server_state, cmd, data_length) do
            {cmd, rest} ->
              prepend_cmd(loop_state, cmd, server_state)
              loop_state = LoopState.new
              server_state = cond do
                size(rest) > 0 -> server_state.existing_data(rest)
                true -> server_state
              end
            _ ->
              loop_state = LoopState.new
              :ok
          end
        {:prepend, _} ->
          Lager.info "bad data length"
          send_error(server_state)
          loop_state = LoopState.new
        {:commands, "cas"} ->
          # Lager.info "cas: #{inspect parts}"
          case parts do
            [_, k, flags, exptime, data_length, cas] when size(cas) > 0->
              # Lager.info "size(cas): #{size(cas)}"
              loop_state = try do
                LoopState[state: :cas, key: k, flags: binary_to_integer(flags), exptime: binary_to_integer(exptime), data_length: binary_to_integer(data_length), cas: binary_to_integer(cas)]
              catch
                :error, :badarg ->
                  send_ascii_reply :bad_command_line, server_state
                  LoopState.new
                what, value ->
                  Lager.info "13> Caught #{inspect what} with #{inspect value}"
                  LoopState.new
              end
            [_, k, flags, exptime, data_length, cas, nr] when size(cas) > 0->
              # Lager.info "size(cas): #{size(cas)}"
              loop_state = try do
                LoopState[state: :cas, key: k, flags: binary_to_integer(flags), exptime: binary_to_integer(exptime), data_length: binary_to_integer(data_length), cas: binary_to_integer(cas), no_reply: nr]
              catch
                :error, :badarg ->
                  send_ascii_reply :bad_command_line, server_state
                  LoopState.new
                what, value ->
                  Lager.info "14> Caught #{inspect what} with #{inspect value}"
                  LoopState.new
              end
            _ ->
              send_error(server_state)
              loop_state = loop_state.state :commands
          end
        {:cas, _} when cmd_size == data_length ->
          cas_cmd(loop_state, cmd, server_state)
          loop_state = LoopState.new
        {:cas, _} when cmd_size < data_length ->
          case read_remainder_ascii(server_state, cmd, data_length) do
            {cmd, rest} ->
              cas_cmd(loop_state, cmd, server_state)
              loop_state = LoopState.new
              server_state = cond do
                size(rest) > 0 -> server_state.existing_data(rest)
                true -> server_state
              end
            _ ->
              loop_state = LoopState.new
              :ok
          end
        {:cas, _} ->
          Lager.info "bad data length"
          send_error(server_state)
          loop_state = LoopState.new
        {:commands, "delete"} ->
          [_|tail] = parts
          delete_cmd(tail, server_state)
          loop_state = LoopState.new
        {:commands, "incr"} ->
          [_|tail] = parts
          incr_cmd(tail, server_state)
          loop_state = LoopState.new
        {:commands, "decr"} ->
          [_|tail] = parts
          decr_cmd(tail, server_state)
          loop_state = LoopState.new
        {:commands, "touch"} ->
          [_|tail] = parts
          touch_cmd(tail, server_state)
          loop_state = LoopState.new
        {:commands, "stats"} ->
          stats_cmd(parts, server_state)
          loop_state = LoopState.new
        {:commands, "shutdown"} ->
          close_transport(server_state)
          loop_state = LoopState.new
        {:commands, "quit"} ->
          close_transport(server_state)
          loop_state = LoopState.new
        {_, _} when cmd_size == 0 ->
          Bd.send_data(server_state, <<"END\r\n">>)
        {_, _} ->
          Lager.info "unknown command: #{cmd} in loop_state: #{inspect loop_state}"
          send_error(server_state)
          loop_state = LoopState.new
      end
      # Lager.info ">>> loop_state: #{inspect loop_state}"
      { server_state, loop_state }
    end
    # Lager.info ">>> server_state: #{inspect server_state}"
    server_state
  end

  defp send_error(server_state) do
    Bd.send_data(server_state, <<"ERROR\r\n">>)
  end

  defp send_ascii_reply {:stored, _}, server_state do
    Bd.send_data(server_state, <<"STORED\r\n">>)
  end

  defp send_ascii_reply :not_stored, server_state do
    Bd.send_data(server_state, <<"NOT_STORED\r\n">>)
  end

  defp send_ascii_reply :exists, server_state do
    Bd.send_data(server_state, <<"EXISTS\r\n">>)
  end

  defp send_ascii_reply :not_found, server_state do
    Bd.send_data(server_state, <<"NOT_FOUND\r\n">>)
  end

  defp send_ascii_reply :deleted, server_state do
    Bd.send_data(server_state, <<"DELETED\r\n">>)
  end

  defp send_ascii_reply :touched, server_state do
    Bd.send_data(server_state, <<"TOUCHED\r\n">>)
  end

  defp send_ascii_reply {value, _cas}, server_state do
    Bd.send_data(server_state, <<"#{value}\r\n">>)
  end

  defp send_ascii_reply :invalid_incr_decr, server_state do
    Bd.send_data(server_state, <<"CLIENT_ERROR cannot increment or decrement non-numeric value\r\n">>)
  end

  defp send_ascii_reply :bad_command_line, server_state do
    Bd.send_data(server_state, <<"CLIENT_ERROR bad command line format\r\n">>)
  end

  defp send_ascii_reply :bad_delete_command_line, server_state do
    Bd.send_data(server_state, <<"CLIENT_ERROR bad command line format.  Usage: delete <key> [noreply]\r\n">>)
  end

  defp send_ascii_reply _, server_state do
    send_error(server_state)
  end

  defp set_cmd(loop_state = LoopState[no_reply: nil], value, server_state) do
    # Lager.info "set_cmd: #{inspect loop_state}"
    send_ascii_reply(MemcachedE.set(loop_state.key, value, loop_state.flags, loop_state.exptime, 0), server_state)
  end

  defp set_cmd(loop_state, value, _server_state) do
    # Lager.info "set_cmd: #{inspect loop_state} nr"
    MemcachedE.set(loop_state.key, value, loop_state.flags, loop_state.exptime, 0)
  end

  defp add_cmd(loop_state = LoopState[no_reply: nil], value, server_state) do
    send_ascii_reply(MemcachedE.add(loop_state.key, value, loop_state.flags, loop_state.exptime), server_state)
  end

  defp add_cmd(loop_state, value, _server_state) do
    MemcachedE.add(loop_state.key, value, loop_state.flags, loop_state.exptime)
  end

  defp replace_cmd(loop_state = LoopState[no_reply: nil], value, server_state) do
    send_ascii_reply(MemcachedE.replace(loop_state.key, value, loop_state.flags, loop_state.exptime), server_state)
  end

  defp replace_cmd(loop_state, value, _server_state) do
    MemcachedE.replace(loop_state.key, value, loop_state.flags, loop_state.exptime)
  end

  defp append_cmd(loop_state = LoopState[no_reply: nil], value, server_state) do
    send_ascii_reply(MemcachedE.append(loop_state.key, value, loop_state.flags, loop_state.exptime), server_state)
  end

  defp append_cmd(loop_state, value, _server_state) do
    MemcachedE.append(loop_state.key, value, loop_state.flags, loop_state.exptime)
  end

  defp prepend_cmd(loop_state = LoopState[no_reply: nil], value, server_state) do
    send_ascii_reply(MemcachedE.prepend(loop_state.key, value, loop_state.flags, loop_state.exptime), server_state)
  end

  defp prepend_cmd(loop_state, value, _server_state) do
    MemcachedE.prepend(loop_state.key, value, loop_state.flags, loop_state.exptime)
  end

  defp cas_cmd(loop_state = LoopState[no_reply: nil], value, server_state) do
    # Lager.info "cas_cmd: #{inspect loop_state} #{value}"
    send_ascii_reply(MemcachedE.set(loop_state.key, value, loop_state.flags, loop_state.exptime, loop_state.cas), server_state)
  end

  defp cas_cmd(loop_state, value, _server_state) do
    # Lager.info "cas_cmd: #{inspect loop_state} nr"
    MemcachedE.set(loop_state.key, value, loop_state.flags, loop_state.exptime, loop_state.cas)
  end

  defp get_cmd([key|tail], server_state) do
    cond do
      size(key) < 250 ->
        buffer = case MemcachedE.get key do
          :not_found ->
            << >>
          {value, flags, _cas} ->
            if is_integer(value), do: value = integer_to_binary value
            <<"VALUE #{key} #{flags} #{size(value)}\r\n">> <> value <> <<"\r\n">>
        end
        get_cmd tail, buffer, server_state
      true ->
        send_ascii_reply :bad_command_line, server_state
    end
  end

  defp get_cmd([key|tail], buffer, server_state) do
    cond do
      size(key) <= 250 ->
        buffer = case MemcachedE.get key do
          :not_found ->
            buffer # skip unfound values
          {value, flags, _cas} ->
            if is_integer(value), do: value = integer_to_binary value
            buffer <> <<"VALUE #{key} #{flags} #{size(value)}\r\n">> <> value <> <<"\r\n">>
        end
        get_cmd tail, buffer, server_state
      true ->
        send_ascii_reply :bad_command_line, server_state
    end
  end

  defp get_cmd([], buffer, server_state) do
    # Lager.info "get_cmd: []"
    Bd.send_data(server_state, buffer <> <<"END\r\n">>)
  end

  defp gets_cmd([key|tail], server_state) do
    # Lager.info "gets_cmd: #{key}"
    buffer = case MemcachedE.get key do
      :not_found ->
        << >> # skip unfound values
      {value, flags, cas} ->
        if is_integer(value), do: value = integer_to_binary value
        <<"VALUE #{key} #{flags} #{size(value)} #{cas}\r\n">> <> value <> <<"\r\n">>
    end
    gets_cmd tail, buffer, server_state
  end

  defp gets_cmd([key|tail], buffer, server_state) do
    # Lager.info "gets_cmd: #{key}"
    buffer = case MemcachedE.get key do
      :not_found ->
        buffer # skip unfound values
      {value, flags, cas} ->
        if is_integer(value), do: value = integer_to_binary value
        buffer <> <<"VALUE #{key} #{flags} #{size(value)} #{cas}\r\n">> <> value <> <<"\r\n">>
    end
    gets_cmd tail, buffer, server_state
  end

  defp gets_cmd([], buffer, server_state) do
    # Lager.info "gets_cmd: [] #{inspect buffer}"
    Bd.send_data(server_state, buffer <> <<"END\r\n">>)
  end

  defp delete_cmd([key], server_state) do
    send_ascii_reply(MemcachedE.delete(key), server_state)
  end

  defp delete_cmd([key, <<"noreply">>], _server_state) do
    MemcachedE.delete(key)
  end

  defp delete_cmd([key, <<"0">>], server_state) do
    send_ascii_reply(MemcachedE.delete(key), server_state)
  end

  defp delete_cmd([key, <<"0">>, <<"noreply">>], _server_state) do
    MemcachedE.delete(key)
  end

  defp delete_cmd([key, _], server_state) do
    send_ascii_reply :bad_delete_command_line, server_state
  end

  defp delete_cmd([key, _, <<"noreply">>], server_state) do
  end

  defp touch_cmd([key, exiration], server_state) do
    send_ascii_reply(MemcachedE.touch(key, exiration), server_state)
  end

  defp touch_cmd([key, exiration, _], _server_state) do
    MemcachedE.touch(key, exiration)
  end

  defp incr_cmd([key], server_state) do
    send_ascii_reply(MemcachedE.incr(key, 1, 0, 0xffffffff), server_state)
  end

  defp incr_cmd([key, count], server_state) do
    send_ascii_reply(MemcachedE.incr(key, binary_to_integer(count), 0, 0xffffffff), server_state)
  end

  defp incr_cmd([key, count, _], _server_state) do
    MemcachedE.incr(key, binary_to_integer(count))
  end

  defp decr_cmd([key], server_state) do
    send_ascii_reply(MemcachedE.decr(key, 1, 0, 0xffffffff), server_state)
  end

  defp decr_cmd([key, count], server_state) do
    send_ascii_reply(MemcachedE.decr(key, binary_to_integer(count), 0, 0xffffffff), server_state)
  end

  defp decr_cmd([key, count, _], _server_state) do
    MemcachedE.incr(key, binary_to_integer(count))
  end

  defp stats_cmd(["stats", "cachedump", "1", "0", "0"], server_state) do
    # Lager.info "stats for cachedump 1 0 0"
    Bd.send_data(server_state, <<"END\r\n">>)
  end

  defp stats_cmd(["stats", "cachedump", "200", "0", "0"], server_state) do
    # Lager.info "stats for cachedump 200 0 0"
    Bd.send_data(server_state, <<"CLIENT_ERROR\r\n">>)
  end

  defp stats_cmd(["stats", "slabs"], server_state) do
    # Lager.info "stats for slabs"
    Bd.send_data(server_state, <<"STAT total_malloced 4294967328\r\n">>)
    Bd.send_data(server_state, <<"STAT active_slabs 0\r\n">>)
    Bd.send_data(server_state, <<"END\r\n">>)
  end

  defp stats_cmd(["stats", value], server_state) do
    Lager.info "stats for #{value}"
    Bd.send_data(server_state, <<"END\r\n">>)
  end

  defp stats_cmd(["stats", value, parameter], server_state) do
    Lager.info "stats for #{value} #{parameter}"
    Bd.send_data(server_state, <<"END\r\n">>)
  end

  defp stats_cmd(["stats"], server_state) do
    # Lager.info "all stats"
    Bd.send_data(server_state, <<"STAT pointer_size 64\r\n">>)
    Bd.send_data(server_state, <<"STAT limit_maxbytes 4297064448\r\n">>)
    Bd.send_data(server_state, <<"END\r\n">>)
  end

  def read_remainder_ascii(server_state, data, expected) do
    len = size(data)
    # Lager.info "read_remainder_ascii: #{expected} - #{len}"
    cond do
      len == expected -> data
      len == expected + 1 -> { String.slice(data, 0..-2), <<"\r">> }
      len > expected ->
        [read, rest] = String.split data, "\r\n", global: false
        {read, rest}
      true ->
        case server_state.transport.recv(server_state.socket, 0, @receive_timeout) do
          {:ok, read} ->
            read_remainder_ascii(server_state, data <> read, expected)
          res ->
            Lager.info "read_remainder_ascii res: #{inspect res}"
            :ok = close_transport(server_state)
            nil
        end
    end
  end

  def read_remainder(server_state, data, expected) do
    cond do
      size(data) == expected -> data
      size(data) > expected ->
        <<data::[binary, size(expected)], rest::binary >> = data
        {data, rest}
      true ->
        case server_state.transport.recv(server_state.socket, 0, @receive_timeout) do
          {:ok, read} ->
            read_remainder(server_state, data <> read, expected)
          res ->
            Lager.info "read_remainder res: #{inspect res}"
            :ok = close_transport(server_state)
            nil
        end
    end
  end

  def read_expected server_state, data, expected do
    len = size(data)
    cond do
      len < expected ->
      case server_state.transport.recv(server_state.socket, expected - len, @receive_timeout) do
        {:ok, read} ->
          data <> read
        res ->
          Lager.info "read_expected res: #{inspect res}"
          :ok = close_transport(server_state)
          nil
      end
      true -> data
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

  defp close_transport server_state do
    :timer.sleep(500)
    server_state.transport.close(server_state.socket)
  end

  def send_stat_response server_state, key, value, opcode, opaque do
    keylen = size(key)
    value_len = size(value)
    B.send_response_header(server_state, opcode, keylen, 0, 0, Bd.protocol_binray_response_success, keylen + value_len, opaque)
    Bd.send_data(server_state, key <> value)
  end

end
