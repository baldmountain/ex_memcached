defmodule ExMemcached.Server do
  use GenServer
  require Logger
  require ExMemcached.BaseDefinitions
  alias ExMemcached.BaseDefinitions, as: Bd
  alias ExMemcached.BinaryCommands, as: B
  alias ExMemcached.AsciiCommands, as: A
  alias ExMemcached.AsciiCommands.LoopState, as: LoopState
  alias ExMemcached.ServerState, as: ServerState

  @receive_timeout 120000

  def start_link(ref, socket, transport, opts) do
    pid = spawn_link(__MODULE__, :init, [ref, socket, transport, opts])
    {:ok, pid}
  end

  def init(ref, socket, transport, _opts \\ []) do
    :ok = :ranch.accept_ack(ref)
    loop(%ServerState{socket: socket, transport: transport})
  end

  defp loop(server_state) do
  	case server_state.transport.recv(server_state.socket, 0, @receive_timeout) do
  		{:ok, data} ->
        data = server_state.existing_data <> data
        server_state = %ServerState{server_state | existing_data: ""}
        server_state = cond do
          String.starts_with?(data, << Bd.protocol_binary_req >>) -> handle_binary_protocol(server_state, data)
          true ->
            cond do
              String.contains?(data, "\r\n") -> handle_ascii_protocol(server_state, data)
              true -> %ServerState{server_state | existing_data: server_state.existing_data <> data}
            end
        end
  			loop(server_state)
  		_res ->
  			:ok = ServerState.close_transport(server_state)
  	end
  end

  defp handle_binary_protocol(server_state, <<>>) do
    server_state
  end

  defp handle_binary_protocol(server_state, data) do
    server_state = try do
      data = read_expected server_state, data, 24
      << Bd.protocol_binary_req, opcode, keylen::big-unsigned-integer-size(16), extlen, datatype, reserved::big-unsigned-integer-size(16),
        bodylen::big-unsigned-integer-size(32), opaque::big-unsigned-integer-size(32), cas::big-unsigned-integer-size(64), tail::binary >> = data
        # Logger.info "#{Bd.opcode_description opcode} keylen #{keylen} extlen #{extlen} datatype #{datatype} reserved #{reserved} bodylen #{bodylen} opaque #{opaque} cas #{cas}"

      if keylen > 250 do
        B.send_error(server_state, opcode, opaque, Bd.protocol_binray_response_e2big)
        ServerState.close_transport(server_state)
      else
        case opcode do
          Bd.protocol_binray_cmd_noop ->
            server_state = B.send_stored_responses server_state
            server_state = B.send_response_header(server_state, opcode, 0, 0, 0, Bd.protocol_binray_response_success, 0, opaque)
          Bd.protocol_binray_cmd_quit ->
            server_state = B.send_stored_responses
            server_state = B.send_response_header(server_state, opcode, 0, 0, 0, Bd.protocol_binray_response_success, 0, opaque)
            ServerState.close_transport(server_state)
          Bd.protocol_binray_cmd_quitq ->
            ServerState.close_transport(server_state)
          Bd.protocol_binray_cmd_set when extlen == 8 ->
            tail = read_expected server_state, tail, extlen + keylen
            data_len = bodylen - extlen - keylen
            << flags::size(32), exptime::size(32), key::binary-size(keylen), data::binary >> = tail
            cond do
              data_len > Application.get_env(:ex_memcached, :max_data_size) ->
                # delete if the key exists
                ExMemcached.delete key
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
            << flags::size(32), exptime::size(32), key::binary-size(keylen), data::binary >> = tail
            cond do
              data_len > Application.get_env(:ex_memcached, :max_data_size) ->
                # delete if the key exists
                ExMemcached.delete key
                case read_remainder(server_state, data, bodylen - extlen - keylen) do
                  {_data, rest} ->
                    server_state = B.send_error(server_state, opcode, opaque, Bd.protocol_binray_response_e2big)
                    server_state = handle_binary_protocol(server_state, rest)
                  _data -> B.send_error(server_state, opcode, opaque, Bd.protocol_binray_response_e2big)
                end
              true ->
                case read_remainder(server_state, data, bodylen - extlen - keylen) do
                  {data, rest} ->
                    ExMemcached.set(key, data, flags, exptime, cas)
                    server_state = handle_binary_protocol(server_state, rest)
                  data -> ExMemcached.set(key, data, flags, exptime, cas)
                end
            end
          Bd.protocol_binray_cmd_add ->
            tail = read_expected server_state, tail, extlen + keylen
            data_len = bodylen - extlen - keylen
            << flags::size(32), exptime::size(32), key::binary-size(keylen), data::binary >> = tail
            cond do
              data_len > Application.get_env(:ex_memcached, :max_data_size) ->
                # delete if the key exists
                ExMemcached.delete key
                server_state = case read_remainder(server_state, data, bodylen - extlen - keylen) do
                  {_data, rest} ->
                    server_state = B.send_stored_responses server_state
                    server_state = B.send_error(server_state, opcode, opaque, Bd.protocol_binray_response_e2big)
                    handle_binary_protocol(server_state, rest)
                  _data -> B.send_error(server_state, opcode, opaque, Bd.protocol_binray_response_e2big)
                end
              true ->
                server_state = case read_remainder(server_state, data, bodylen - extlen - keylen) do
                  {data, rest} ->
                    server_state = B.send_stored_responses server_state
                    server_state = B.binary_add_cmd(key, data, flags, exptime, opcode, opaque, server_state)
                    handle_binary_protocol(server_state, rest)
                  data -> B.binary_add_cmd(key, data, flags, exptime, opcode, opaque, server_state)
                end
            end
          Bd.protocol_binray_cmd_addq ->
            tail = read_expected server_state, tail, extlen + keylen
            data_len = bodylen - extlen - keylen
            << flags::size(32), exptime::size(32), key::binary-size(keylen), data::binary >> = tail
            cond do
              data_len > Application.get_env(:ex_memcached, :max_data_size) ->
                # delete if the key exists
                ExMemcached.delete key
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
            << flags::size(32), exptime::size(32), key::binary-size(keylen), data::binary >> = tail
            cond do
              data_len > Application.get_env(:ex_memcached, :max_data_size) ->
                # delete if the key exists
                ExMemcached.delete key
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
            << flags::size(32), exptime::size(32), key::binary-size(keylen), data::binary >> = tail
            cond do
              data_len > Application.get_env(:ex_memcached, :max_data_size) ->
                # delete if the key exists
                ExMemcached.delete key
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
            << count::unsigned-size(64), intial::size(64), expiration::unsigned-integer-size(32), key::binary-size(keylen), data::binary >> = tail
            server_state = B.binary_incr_cmd(key, count, intial, expiration, opcode, opaque, server_state)
            server_state = handle_binary_protocol(server_state, data)
          Bd.protocol_binray_cmd_incrementq ->
            tail = read_expected server_state, tail, extlen + keylen
            << count::size(64), intial::size(64), expiration::unsigned-integer-size(32), key::binary-size(keylen), data::binary >> = tail
            server_state = B.binary_incrq_cmd(key, count, intial, expiration, opcode, opaque, server_state)
            server_state = handle_binary_protocol(server_state, data)
          Bd.protocol_binray_cmd_decrement ->
            tail = read_expected server_state, tail, extlen + keylen
            << count::size(64), intial::size(64), expiration::unsigned-integer-size(32), key::binary-size(keylen), data::binary >> = tail
            server_state = B.binary_decr_cmd(key, count, intial, expiration, opcode, opaque, server_state)
            server_state = handle_binary_protocol(server_state, data)
          Bd.protocol_binray_cmd_decrementq ->
            tail = read_expected server_state, tail, extlen + keylen
            << count::size(64), intial::size(64), expiration::unsigned-integer-size(32), key::binary-size(keylen), data::binary >> = tail
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
                tail = read_expected server_state, tail, extlen
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
            << exptime::size(32), key::binary-size(keylen), data::binary >> = tail
            server_state = B.binary_touch_cmd(key, exptime, opcode, opaque, server_state)
            server_state = handle_binary_protocol(server_state, data)
          Bd.protocol_binray_cmd_gat ->
            tail = read_expected server_state, tail, extlen + keylen
            << exptime::size(32), key::binary-size(keylen), data::binary >> = tail
            server_state = B.binary_gat_cmd(key, exptime, opcode, opaque, server_state)
            server_state = handle_binary_protocol(server_state, data)
          Bd.protocol_binray_cmd_stat ->
            {key, data} = key_data(extlen, keylen, tail)
            case key do
              "settings" ->
                send_stat_response server_state, "maxconns", <<"#{Application.get_env(:ex_memcached, :max_connections)}">>, opcode, opaque
                send_stat_response server_state, "domain_socket", "NULL", opcode, opaque
                send_stat_response server_state, "evictions", "on", opcode, opaque
                send_stat_response server_state, "cas_enabled", "yes", opcode, opaque
                case Application.get_env(:ex_memcached, :disable_flush_all) do
                  true -> send_stat_response server_state, "flush_enabled", "no", opcode, opaque
                  false -> send_stat_response server_state, "flush_enabled", "yes", opcode, opaque
                end
                B.send_response_header(server_state, opcode, 0, 0, 0, Bd.protocol_binray_response_success, 0, opaque)
              _ ->
                stats = ExMemcached.stats
                {ms,sec,_} = :os.timestamp
                now = ms*1000000+sec
                {_, state} = :application.get_all_key :ex_memcached
                Enum.each([
                  {"pointer_size", 64},
                  {"time", now},
                  {"version", "#{state[:vsn]}"},
                  {"curr_items", stats.curr_items},
                  {"bytes", stats.bytes},
                  {"get_hits", stats.curr_items},
                  {"get_misses", stats.curr_items},
                  {"limit_maxbytes", 4297064448},
                ], fn ({k,v})-> send_stat_response(server_state, k, v, opcode, opaque) end)

                B.send_response_header(server_state, opcode, 0, 0, 0, Bd.protocol_binray_response_success, 0, opaque)
              end
            server_state = handle_binary_protocol(server_state, data)
          _ ->
            Logger.info "Unknown opcode: #{opcode}"
            {_key, data} = key_data(extlen, keylen, tail)
            B.send_response_header(server_state, opcode, 0, 0, 0, Bd.protocol_binray_response_unknown_command, 0, opaque)
            server_state = handle_binary_protocol(server_state, data)
        end
      end
      server_state
    catch
      :error, :badmatch ->
        Logger.info "Badmatch on input command"
        << _magic, opcode, _tail::binary >> = data
        B.send_response_header(server_state, opcode, 0, 0, 0, Bd.protocol_binray_response_einval, 0, 0)
      :exit, value ->
        Logger.info "exit called with #{inspect value}"
        server_state
      :throw, value ->
        Logger.info "Throw called with #{inspect value}"
        server_state
      what, value ->
        Logger.info "1> Caught #{what} with #{inspect value}"
        try do
          << _magic, opcode, _keylen::big-unsigned-integer-size(16), _extlen, _datatype, _reserved::big-unsigned-integer-size(16),
            _bodylen::big-unsigned-integer-size(32), opaque::big-unsigned-integer-size(32), _tail::binary >> = data
          B.send_response_header(server_state, opcode, 0, 0, 0, Bd.protocol_binray_response_einval, 0, opaque)
          server_state
        catch
          what, value ->
            Logger.info "2> Caught secondary #{inspect what} with #{inspect value}"
            server_state
        end
    end
    server_state
  end

  defp handle_ascii_protocol(server_state, data) do
    cmds = String.split data, "\r\n"
    if byte_size(List.last(cmds)) == 0, do: cmds = List.delete_at(cmds, -1)
    # Logger.info "cmds: #{inspect cmds}"

    {server_state, _} = Enum.reduce cmds, {server_state, %LoopState{} }, fn(cmd, {server_state, loop_state}) ->
      command = if (loop_state.state == :commands) do
        parts = String.split cmd, " "
        # Logger.info ">>> #{inspect parts}"
        List.first(parts)
      end
      # for guards
      cmd_size = byte_size(cmd)
      data_length = loop_state.data_length
      case {loop_state.state, command} do
        {:commands, "get"} ->
          [_|tail] = parts
          A.get_cmd(tail, server_state)
          loop_state = %LoopState{}
        {:commands, "gets"} ->
          [_|tail] = parts
          A.gets_cmd(tail, server_state)
          loop_state = %LoopState{}
        {:commands, "set"} ->
          case parts do
            [_, k, flags, exptime, data_length] ->
              loop_state = try do
                %LoopState{state: :set, key: k, flags: :erlang.binary_to_integer(flags), exptime: :erlang.binary_to_integer(exptime), data_length: :erlang.binary_to_integer(data_length)}
              catch
                :error, :badarg ->
                  A.send_ascii_reply :bad_command_line, server_state
                  %LoopState{}
                what, value ->
                  # Logger.info "3> Caught #{inspect what} with #{inspect value}"
                  %LoopState{}
              end
            [_, k, flags, exptime, data_length, nr] ->
              loop_state = try do
                %LoopState{state: :set, key: k, flags: :erlang.binary_to_integer(flags), exptime: :erlang.binary_to_integer(exptime), data_length: :erlang.binary_to_integer(data_length), no_reply: nr}
              catch
                :error, :badarg ->
                  A.send_ascii_reply :bad_command_line, server_state
                  %LoopState{}
                what, value ->
                  # Logger.info "4> Caught #{inspect what} with #{inspect value}"
                  %LoopState{}
              end
            _ ->
              A.send_error(server_state)
              loop_state = %LoopState{}
          end
        {:set, _} when cmd_size == data_length ->
          cond do
            data_length <= Application.get_env(:ex_memcached, :max_data_size) ->
              A.set_cmd(loop_state, cmd, server_state)
            true ->
              Logger.info "Object #{loop_state.key} too big for cache"
              ExMemcached.delete loop_state.key
              ServerState.send_data(server_state, <<"SERVER_ERROR object too large for cache\r\n">>)
          end
          loop_state = %LoopState{}
        {:set, _} ->
          case read_remainder_ascii(server_state, cmd, data_length) do
            {cmd, rest} ->
              cond do
                data_length <= Application.get_env(:ex_memcached, :max_data_size) ->
                  A.set_cmd(loop_state, cmd, server_state)
                true ->
                  Logger.info "Object #{loop_state.key} too big for cache"
                  ExMemcached.delete loop_state.key
                  ServerState.send_data(server_state, <<"SERVER_ERROR object too large for cache\r\n">>)
              end
              server_state = cond do
                byte_size(rest) > 0 ->
                  %ServerState{server_state | existing_data: rest}
                true -> server_state
              end
            cmd ->
              cond do
                data_length <= Application.get_env(:ex_memcached, :max_data_size) ->
                  # Logger.info "setting key: #{loop_state.key} size: #{byte_size(cmd)}"
                  A.set_cmd(loop_state, cmd, server_state)
                true ->
                  Logger.info "Object #{loop_state.key} too big for cache"
                  ExMemcached.delete loop_state.key
                  ServerState.send_data(server_state, <<"SERVER_ERROR object too large for cache\r\n">>)
              end
          end
          loop_state = %LoopState{}
        {:commands, "add"} ->
          case parts do
            [_, k, flags, exptime, data_length] ->
              loop_state = try do
                %LoopState{state: :add, key: k, flags: :erlang.binary_to_integer(flags), exptime: :erlang.binary_to_integer(exptime), data_length: :erlang.binary_to_integer(data_length)}
              catch
                :error, :badarg ->
                  A.send_ascii_reply :bad_command_line, server_state
                  %LoopState{}
                what, value ->
                  Logger.info "5> Caught #{inspect what} with #{inspect value}"
                  %LoopState{}
              end
            [_, k, flags, exptime, data_length, nr] ->
              loop_state = try do
                %LoopState{state: :add, key: k, flags: :erlang.binary_to_integer(flags), exptime: :erlang.binary_to_integer(exptime), data_length: :erlang.binary_to_integer(data_length), no_reply: nr}
              catch
                :error, :badarg ->
                  A.send_ascii_reply :bad_command_line, server_state
                  %LoopState{}
                what, value ->
                  Logger.info "6> Caught #{inspect what} with #{inspect value}"
                  %LoopState{}
              end
            _ ->
              A.send_error(server_state)
              loop_state = %LoopState{}
          end
        {:add, _} when cmd_size == data_length ->
          A.add_cmd(loop_state, cmd, server_state)
          loop_state = %LoopState{}
        {:add, _} when cmd_size < data_length ->
          case read_remainder_ascii(server_state, cmd, data_length) do
            {cmd, rest} ->
              A.add_cmd(loop_state, cmd, server_state)
              server_state = cond do
                byte_size(rest) > 0 -> %ServerState{server_state | existing_data: rest}
                true -> server_state
              end
            cmd ->
              A.add_cmd(loop_state, cmd, server_state)
          end
          loop_state = %LoopState{}
        {:add, _} ->
          Logger.info "bad data length"
          A.send_error(server_state)
          loop_state = %LoopState{}
        {:commands, "replace"} ->
          case parts do
            [_, k, flags, exptime, data_length] ->
              loop_state = try do
                %LoopState{state: :replace, key: k, flags: :erlang.binary_to_integer(flags), exptime: :erlang.binary_to_integer(exptime), data_length: :erlang.binary_to_integer(data_length)}
              catch
                :error, :badarg ->
                  A.send_ascii_reply :bad_command_line, server_state
                  %LoopState{}
                what, value ->
                  Logger.info "7> Caught #{inspect what} with #{inspect value}"
                  %LoopState{}
              end
            [_, k, flags, exptime, data_length, nr] ->
              loop_state = try do
                %LoopState{state: :replace, key: k, flags: :erlang.binary_to_integer(flags), exptime: :erlang.binary_to_integer(exptime), data_length: :erlang.binary_to_integer(data_length), no_reply: nr}
              catch
                :error, :badarg ->
                  A.send_ascii_reply :bad_command_line, server_state
                  %LoopState{}
                what, value ->
                  Logger.info "8> Caught #{inspect what} with #{inspect value}"
                  %LoopState{}
              end
            _ ->
              A.send_error(server_state)
              loop_state = %LoopState{}
          end
        {:replace, _} when cmd_size == data_length ->
          A.replace_cmd(loop_state, cmd, server_state)
          loop_state = %LoopState{}
        {:replace, _} when cmd_size < data_length ->
          case read_remainder_ascii(server_state, cmd, data_length) do
            {cmd, rest} ->
              A.replace_cmd(loop_state, cmd, server_state)
                server_state = cond do
                byte_size(rest) > 0 -> %ServerState{server_state | existing_data: rest}
                true -> server_state
              end
            cmd
              A.replace_cmd(loop_state, cmd, server_state)
          end
          loop_state = %LoopState{}
        {:replace, _} ->
          Logger.info "bad data length"
          A.send_error(server_state)
          loop_state = %LoopState{}
        {:commands, "append"} ->
          case parts do
            [_, k, flags, exptime, data_length] ->
              loop_state = try do
                %LoopState{state: :append, key: k, flags: :erlang.binary_to_integer(flags), exptime: :erlang.binary_to_integer(exptime), data_length: :erlang.binary_to_integer(data_length)}
              catch
                :error, :badarg ->
                  A.send_ascii_reply :bad_command_line, server_state
                  %LoopState{}
                what, value ->
                  Logger.info "9> Caught #{inspect what} with #{inspect value}"
                  %LoopState{}
              end
            [_, k, flags, exptime, data_length, nr] ->
              loop_state = try do
                %LoopState{state: :append, key: k, flags: :erlang.binary_to_integer(flags), exptime: :erlang.binary_to_integer(exptime), data_length: :erlang.binary_to_integer(data_length), no_reply: nr}
              catch
                :error, :badarg ->
                  A.send_ascii_reply :bad_command_line, server_state
                  %LoopState{}
                what, value ->
                  Logger.info "10> Caught #{inspect what} with #{inspect value}"
                  %LoopState{}
              end
            _ ->
              A.send_error(server_state)
              loop_state = %LoopState{}
          end
        {:append, _} when cmd_size == data_length ->
          A.append_cmd(loop_state, cmd, server_state)
          loop_state = %LoopState{}
        {:append, _} when cmd_size < data_length ->
          case read_remainder_ascii(server_state, cmd, data_length) do
            {cmd, rest} ->
              A.append_cmd(loop_state, cmd, server_state)
              server_state = cond do
                byte_size(rest) > 0 -> %ServerState{server_state | existing_data: rest}
                true -> server_state
              end
            cmd ->
              A.append_cmd(loop_state, cmd, server_state)
          end
          loop_state = %LoopState{}
        {:append, _} ->
          Logger.info "bad data length"
          A.send_error(server_state)
          loop_state = %LoopState{}
        {:commands, "prepend"} ->
          case parts do
            [_, k, flags, exptime, data_length] ->
              loop_state = try do
                %LoopState{state: :prepend, key: k, flags: :erlang.binary_to_integer(flags), exptime: :erlang.binary_to_integer(exptime), data_length: :erlang.binary_to_integer(data_length)}
              catch
                :error, :badarg ->
                  A.send_ascii_reply :bad_command_line, server_state
                  %LoopState{}
                what, value ->
                  Logger.info "11> Caught #{inspect what} with #{inspect value}"
                  %LoopState{}
              end
            [_, k, flags, exptime, data_length, nr] ->
              loop_state = try do
                %LoopState{state: :prepend, key: k, flags: :erlang.binary_to_integer(flags), exptime: :erlang.binary_to_integer(exptime), data_length: :erlang.binary_to_integer(data_length), no_reply: nr}
              catch
                :error, :badarg ->
                  A.send_ascii_reply :bad_command_line, server_state
                  %LoopState{}
                what, value ->
                  Logger.info "12> Caught #{inspect what} with #{inspect value}"
                  %LoopState{}
              end
            _ ->
              A.send_error(server_state)
              loop_state = %LoopState{}
          end
        {:prepend, _} when cmd_size == data_length ->
          A.prepend_cmd(loop_state, cmd, server_state)
          loop_state = %LoopState{}
        {:prepend, _} when cmd_size < data_length ->
          case read_remainder_ascii(server_state, cmd, data_length) do
            {cmd, rest} ->
              A.prepend_cmd(loop_state, cmd, server_state)
              server_state = cond do
                byte_size(rest) > 0 -> %ServerState{server_state | existing_data: rest}
                true -> server_state
              end
            cmd ->
              A.prepend_cmd(loop_state, cmd, server_state)
          end
          loop_state = %LoopState{}
        {:prepend, _} ->
          Logger.info "bad data length"
          A.send_error(server_state)
          loop_state = %LoopState{}
        {:commands, "cas"} ->
          case parts do
            [_, k, flags, exptime, data_length, cas] when byte_size(cas) > 0->
              loop_state = try do
                %LoopState{state: :cas, key: k, flags: :erlang.binary_to_integer(flags), exptime: :erlang.binary_to_integer(exptime), data_length: :erlang.binary_to_integer(data_length), cas: :erlang.binary_to_integer(cas)}
              catch
                :error, :badarg ->
                  A.send_ascii_reply :bad_command_line, server_state
                  %LoopState{}
                what, value ->
                  Logger.info "13> Caught #{inspect what} with #{inspect value}"
                  %LoopState{}
              end
            [_, k, flags, exptime, data_length, cas, nr] when byte_size(cas) > 0->
              loop_state = try do
                %LoopState{state: :cas, key: k, flags: :erlang.binary_to_integer(flags), exptime: :erlang.binary_to_integer(exptime), data_length: :erlang.binary_to_integer(data_length), cas: :erlang.binary_to_integer(cas), no_reply: nr}
              catch
                :error, :badarg ->
                  A.send_ascii_reply :bad_command_line, server_state
                  %LoopState{}
                what, value ->
                  Logger.info "14> Caught #{inspect what} with #{inspect value}"
                  %LoopState{}
              end
            _ ->
              A.send_error(server_state)
              loop_state = %LoopState{loop_state | state: :commands}
          end
        {:cas, _} when cmd_size == data_length ->
          A.cas_cmd(loop_state, cmd, server_state)
          loop_state = %LoopState{}
        {:cas, _} when cmd_size < data_length ->
          case read_remainder_ascii(server_state, cmd, data_length) do
            {cmd, rest} ->
              A.cas_cmd(loop_state, cmd, server_state)
              server_state = cond do
                byte_size(rest) > 0 -> %ServerState{ server_state | existing_data: rest}
                true -> server_state
              end
            cmd ->
              A.cas_cmd(loop_state, cmd, server_state)
          end
          loop_state = %LoopState{}
        {:cas, _} ->
          Logger.info "bad data length"
          A.send_error(server_state)
          loop_state = %LoopState{}
        {:commands, "delete"} ->
          [_|tail] = parts
          A.delete_cmd(tail, server_state)
          loop_state = %LoopState{}
        {:commands, "incr"} ->
          [_|tail] = parts
          A.incr_cmd(tail, server_state)
          loop_state = %LoopState{}
        {:commands, "decr"} ->
          [_|tail] = parts
          A.decr_cmd(tail, server_state)
          loop_state = %LoopState{}
        {:commands, "touch"} ->
          [_|tail] = parts
          A.touch_cmd(tail, server_state)
          loop_state = %LoopState{}
        {:commands, "flush_all"} ->
          [_|tail] = parts
          A.flush_all_cmd(tail, server_state)
          loop_state = %LoopState{}
        {:commands, "stats"} ->
          A.stats_cmd(parts, server_state)
        {:commands, "verbosity"} ->
          A.verbosity_cmd(parts, server_state)
          loop_state = %LoopState{}
        {:commands, "shutdown"} ->
          ServerState.close_transport(server_state)
          loop_state = %LoopState{}
        {:commands, "quit"} ->
          ServerState.close_transport(server_state)
          loop_state = %LoopState{}
        {:commands, << >>} when cmd_size == 0 ->
          # blank so just skip to next one
          loop_state = %LoopState{}
        {_, _} ->
          Logger.info "unknown command: #{cmd} in loop_state: #{inspect loop_state}"
          dump_state server_state, loop_state
          A.send_error(server_state)
          # ServerState.close_transport(server_state)
          loop_state = %LoopState{}
      end
      # Logger.info ">>> loop_state: #{inspect loop_state}"
      { server_state, loop_state }
    end
    # Logger.info ">>> server_state: #{inspect server_state}"
    server_state
  end

  def read_remainder_ascii(server_state, data, expected) do
    buf_len = expected + 2
    len = byte_size(data)
    remaining = buf_len - len
    # Logger.info "read_remainder_ascii data len: #{len} buf_len: #{buf_len}"
    cond do
      remaining < 0 ->
        << result::binary-size(expected), crlf::binary-size(2), rest::binary >> = data
        # :io.format("read_remainder_ascii crlf '~w'~n", [crlf])
        {result, rest}
      remaining > 0 ->
        cond do
          remaining > 16384 ->
            case server_state.transport.recv(server_state.socket, 16384, @receive_timeout) do
              {:ok, read} ->
                read_remainder_ascii(server_state, data <> read, expected)
              res ->
                Logger.info "read_remainder_ascii res: #{inspect res}"
                :ok = ServerState.close_transport(server_state)
                nil
            end
          true ->
            case server_state.transport.recv(server_state.socket, remaining, @receive_timeout) do
              {:ok, read} ->
                read_remainder_ascii(server_state, data <> read, expected)
              res ->
                Logger.info "read_remainder_ascii res: #{inspect res}"
                :ok = ServerState.close_transport(server_state)
                nil
            end
        end
      2 == buf_len -> { <<>>, binary_part(data, 2, len-2) }
      remaining == 0 ->
        # binary_part data, 0, expected
        << result::binary-size(expected), rest::binary >> = data
        # :io.format("read_remainder_ascii rest '~w'~n", [binary_part(data, buf_len, -100)])
        result
      len > buf_len -> { binary_part(data, 0, buf_len), binary_part(data, buf_len, len-buf_len) }
    end
  end

  def read_remainder(server_state, data, expected) do
    len = byte_size(data)
    cond do
      len == expected -> data
      len > expected ->
        <<data::binary-size(expected), rest::binary >> = data
        {data, rest}
      true ->
        case server_state.transport.recv(server_state.socket, expected - len, @receive_timeout) do
          {:ok, read} ->
            read_remainder(server_state, data <> read, expected)
          res ->
            Logger.info "read_remainder res: #{inspect res}"
            :ok = ServerState.close_transport(server_state)
            nil
        end
    end
  end

  def read_expected server_state, data, expected do
    len = byte_size(data)
    cond do
      len < expected ->
      case server_state.transport.recv(server_state.socket, expected - len, @receive_timeout) do
        {:ok, read} ->
          data <> read
        res ->
          Logger.info "read_expected res: #{inspect res}"
          :ok = ServerState.close_transport(server_state)
          nil
      end
      true -> data
    end
  end

  defp key_data(extlen, keylen, buffer) do
    case extlen do
      0 -> << key::binary-size(keylen), data::binary >> = buffer
      _ ->
        extlen = extlen * 8
        << _::size(extlen), key::binary-size(keylen), data::binary >> = buffer
    end
    {key, data}
  end

  def send_stat_response(server_state, key, value, opcode, opaque) when is_binary(value) do
    keylen = byte_size(key)
    value_len = byte_size(value)
    B.send_response_header(server_state, opcode, keylen, 0, 0, Bd.protocol_binray_response_success, keylen + value_len, opaque)
    ServerState.send_data(server_state, key <> value)
  end

  def send_stat_response server_state, key, value, opcode, opaque do
    value = :erlang.integer_to_binary(value)
    keylen = byte_size(key)
    value_len = byte_size(value)
    B.send_response_header(server_state, opcode, keylen, 0, 0, Bd.protocol_binray_response_success, keylen + value_len, opaque)
    ServerState.send_data(server_state, key <> value)
  end

  def dump_state server_state, nil do
    Logger.info ">>> server_state: #{inspect server_state}"
  end

  def dump_state server_state, loop_state do
    Logger.info ">>> server_state: #{inspect server_state}"
    Logger.info ">>> loop_state: #{inspect loop_state}"
  end
end
