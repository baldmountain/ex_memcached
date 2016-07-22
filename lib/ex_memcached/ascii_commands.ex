defmodule ExMemcached.AsciiCommands do
  require ExMemcached.BaseDefinitions
  alias ExMemcached.ServerState, as: ServerState
  require Logger

  defmodule LoopState do
    defstruct state: :commands, key: nil, flags: 0, exptime: 0, cas: 0, data_length: 0, no_reply: nil
  end

  def send_error(server_state) do
    ServerState.send_data(server_state, <<"ERROR\r\n">>)
  end

  def send_ascii_reply {:stored, _}, server_state do
    ServerState.send_data(server_state, <<"STORED\r\n">>)
  end

  def send_ascii_reply :not_stored, server_state do
    ServerState.send_data(server_state, <<"NOT_STORED\r\n">>)
  end

  def send_ascii_reply :exists, server_state do
    ServerState.send_data(server_state, <<"EXISTS\r\n">>)
  end

  def send_ascii_reply :not_found, server_state do
    ServerState.send_data(server_state, <<"NOT_FOUND\r\n">>)
  end

  def send_ascii_reply :deleted, server_state do
    ServerState.send_data(server_state, <<"DELETED\r\n">>)
  end

  def send_ascii_reply :touched, server_state do
    ServerState.send_data(server_state, <<"TOUCHED\r\n">>)
  end

  def send_ascii_reply :ok, server_state do
    ServerState.send_data(server_state, <<"OK\r\n">>)
  end

  def send_ascii_reply {value, _cas}, server_state do
    ServerState.send_data(server_state, <<"#{value}\r\n">>)
  end

  def send_ascii_reply :invalid_incr_decr, server_state do
    ServerState.send_data(server_state, <<"CLIENT_ERROR cannot increment or decrement non-numeric value\r\n">>)
  end

  def send_ascii_reply :bad_command_line, server_state do
    ServerState.send_data(server_state, <<"CLIENT_ERROR bad command line format\r\n">>)
  end

  def send_ascii_reply :bad_delete_command_line, server_state do
    ServerState.send_data(server_state, <<"CLIENT_ERROR bad command line format.  Usage: delete <key> [noreply]\r\n">>)
  end

  def send_ascii_reply :flush_disabled, server_state do
    ServerState.send_data(server_state, <<"CLIENT_ERROR flush_all not allowed\r\n">>)
  end

  def send_ascii_reply :error, server_state do
    ServerState.send_data(server_state, <<"SERVER_ERROR unknown error\r\n">>)
  end

  def send_ascii_reply _, server_state do
    send_error(server_state)
  end

  def set_cmd(loop_state = %LoopState{no_reply: nil}, value, server_state) do
    send_ascii_reply(ExMemcached.set(loop_state.key, value, loop_state.flags, loop_state.exptime, 0), server_state)
  end

  def set_cmd(loop_state, value, _server_state) do
    ExMemcached.set(loop_state.key, value, loop_state.flags, loop_state.exptime, 0)
  end

  def add_cmd(loop_state = %LoopState{no_reply: nil}, value, server_state) do
    send_ascii_reply(ExMemcached.add(loop_state.key, value, loop_state.flags, loop_state.exptime), server_state)
  end

  def add_cmd(loop_state, value, _server_state) do
    ExMemcached.add(loop_state.key, value, loop_state.flags, loop_state.exptime)
  end

  def replace_cmd(loop_state = %LoopState{no_reply: nil}, value, server_state) do
    send_ascii_reply(ExMemcached.replace(loop_state.key, value, loop_state.flags, loop_state.exptime), server_state)
  end

  def replace_cmd(loop_state, value, _server_state) do
    ExMemcached.replace(loop_state.key, value, loop_state.flags, loop_state.exptime)
  end

  def append_cmd(loop_state = %LoopState{no_reply: nil}, value, server_state) do
    send_ascii_reply(ExMemcached.append(loop_state.key, value, loop_state.flags, loop_state.exptime), server_state)
  end

  def append_cmd(loop_state, value, _server_state) do
    ExMemcached.append(loop_state.key, value, loop_state.flags, loop_state.exptime)
  end

  def prepend_cmd(loop_state = %LoopState{no_reply: nil}, value, server_state) do
    send_ascii_reply(ExMemcached.prepend(loop_state.key, value, loop_state.flags, loop_state.exptime), server_state)
  end

  def prepend_cmd(loop_state, value, _server_state) do
    ExMemcached.prepend(loop_state.key, value, loop_state.flags, loop_state.exptime)
  end

  def cas_cmd(loop_state = %LoopState{no_reply: nil}, value, server_state) do
    case loop_state.cas do
      # this is a special case a REAL cas wll neve be 0. This means someone sent an invalid cas. Since
      # we use the same backend as the biary commands a zero means don't check cas so send a -1 instead
      0 -> send_ascii_reply(ExMemcached.set(loop_state.key, value, loop_state.flags, loop_state.exptime, -1), server_state)
      _ -> send_ascii_reply(ExMemcached.set(loop_state.key, value, loop_state.flags, loop_state.exptime, loop_state.cas), server_state)
    end
  end

  def cas_cmd(loop_state, value, _server_state) do
    ExMemcached.set(loop_state.key, value, loop_state.flags, loop_state.exptime, loop_state.cas)
  end

  def get_cmd([key|tail], server_state) do
    cond do
      byte_size(key) < 250 ->
        buffer = case ExMemcached.get key do
          :not_found ->
            << >>
          {value, flags, _cas} ->
            value = if is_integer(value), do: :erlang.integer_to_binary(value), else: value
            <<"VALUE #{key} #{flags} #{byte_size(value)}\r\n">> <> value <> <<"\r\n">>
        end
        get_cmd tail, buffer, server_state
      true ->
        send_ascii_reply :bad_command_line, server_state
    end
  end

  def get_cmd([key|tail], buffer, server_state) do
    cond do
      byte_size(key) <= 250 ->
        buffer = case ExMemcached.get key do
          :not_found ->
            buffer # skip unfound values
          {value, flags, _cas} ->
            value = if is_integer(value), do: :erlang.integer_to_binary(value), else: value
            buffer <> <<"VALUE #{key} #{flags} #{byte_size(value)}\r\n">> <> value <> <<"\r\n">>
        end
        get_cmd tail, buffer, server_state
      true ->
        send_ascii_reply :bad_command_line, server_state
    end
  end

  def get_cmd([], buffer, server_state) do
    ServerState.send_data(server_state, buffer <> <<"END\r\n">>)
  end

  def gets_cmd([key|tail], server_state) do
    buffer = case ExMemcached.get key do
      :not_found ->
        << >> # skip unfound values
      {value, flags, cas} ->
        value = if is_integer(value), do: :erlang.integer_to_binary(value), else: value
        <<"VALUE #{key} #{flags} #{byte_size(value)} #{cas}\r\n">> <> value <> <<"\r\n">>
    end
    gets_cmd tail, buffer, server_state
  end

  def gets_cmd([key|tail], buffer, server_state) do
    buffer = case ExMemcached.get key do
      :not_found ->
        buffer # skip unfound values
      {value, flags, cas} ->
        value = if is_integer(value), do: :erlang.integer_to_binary(value), else: value
        buffer <> <<"VALUE #{key} #{flags} #{byte_size(value)} #{cas}\r\n">> <> value <> <<"\r\n">>
    end
    gets_cmd tail, buffer, server_state
  end

  def gets_cmd([], buffer, server_state) do
    ServerState.send_data(server_state, buffer <> <<"END\r\n">>)
  end

  def delete_cmd([key], server_state) do
    send_ascii_reply(ExMemcached.delete(key), server_state)
  end

  def delete_cmd([key, <<"noreply">>], _server_state) do
    ExMemcached.delete(key)
  end

  def delete_cmd([key, <<"0">>], server_state) do
    send_ascii_reply(ExMemcached.delete(key), server_state)
  end

  def delete_cmd([key, <<"0">>, <<"noreply">>], _server_state) do
    ExMemcached.delete(key)
  end

  def delete_cmd([_key, _], server_state) do
    send_ascii_reply :bad_delete_command_line, server_state
  end

  def delete_cmd([_key, _, <<"noreply">>], _server_state) do
  end

  def touch_cmd([key, exiration], server_state) do
    send_ascii_reply(ExMemcached.touch(key, exiration), server_state)
  end

  def touch_cmd([key, exiration, <<"noreply">>], _server_state) do
    ExMemcached.touch(key, exiration)
  end

  def flush_all_cmd([], server_state) do
    case Application.get_env(:ex_memcached, :disable_flush_all) do
      true -> send_ascii_reply :flush_disabled, server_state
      false -> send_ascii_reply(ExMemcached.flush(0), server_state)
    end
  end

  def flush_all_cmd([expiration], server_state) do
    case Application.get_env(:ex_memcached, :disable_flush_all) do
      true -> send_ascii_reply :flush_disabled, server_state
      false ->
        case expiration do
          <<"noreply">> -> ExMemcached.flush(0)
          _ -> send_ascii_reply(ExMemcached.flush(expiration), server_state)
        end
    end
  end

  def flush_all_cmd([expiration, <<"noreply">>], server_state) do
    case Application.get_env(:ex_memcached, :disable_flush_all) do
      true -> send_ascii_reply :flush_disabled, server_state
      false -> ExMemcached.flush(expiration)
    end
  end

  def incr_cmd([key], server_state) do
    send_ascii_reply(ExMemcached.incr(key, 1, 0, 0xffffffff), server_state)
  end

  def incr_cmd([key, count], server_state) do
    send_ascii_reply(ExMemcached.incr(key, :erlang.binary_to_integer(count), 0, 0xffffffff), server_state)
  end

  def incr_cmd([key, count, _], _server_state) do
    ExMemcached.incr(key, :erlang.binary_to_integer(count), 0, 0xffffffff)
  end

  def decr_cmd([key], server_state) do
    send_ascii_reply(ExMemcached.decr(key, 1, 0, 0xffffffff), server_state)
  end

  def decr_cmd([key, count], server_state) do
    send_ascii_reply(ExMemcached.decr(key, :erlang.binary_to_integer(count), 0, 0xffffffff), server_state)
  end

  def decr_cmd([key, count, _], _server_state) do
    ExMemcached.decr(key, :erlang.binary_to_integer(count), 0, 0xffffffff)
  end

  def verbosity_cmd([_cmd, _value], server_state) do
    ServerState.send_data(server_state, <<"OK\r\n">>)
  end

  def verbosity_cmd([_cmd, _value, <<"noreply">>], _server_state) do
  end

  def stats_cmd(["stats", "cachedump", "1", "0", "0"], server_state) do
    ServerState.send_data(server_state, <<"END\r\n">>)
  end

  def stats_cmd(["stats", "cachedump", "200", "0", "0"], server_state) do
    ServerState.send_data(server_state, <<"CLIENT_ERROR\r\n">>)
  end

  def stats_cmd(["stats", "slabs"], server_state) do
    ServerState.send_data(server_state, <<"STAT total_malloced 4294967328\r\n">>)
    ServerState.send_data(server_state, <<"STAT active_slabs 0\r\n">>)
    ServerState.send_data(server_state, <<"END\r\n">>)
  end

  def stats_cmd(["stats", _value], server_state) do
    ServerState.send_data(server_state, <<"END\r\n">>)
  end

  def stats_cmd(["stats", _value, _parameter], server_state) do
    ServerState.send_data(server_state, <<"END\r\n">>)
  end

  def stats_cmd(["stats"], server_state) do
    stats = ExMemcached.stats
    {ms,sec,_} = :os.timestamp
    now = ms*1000000+sec
    {_, state} = :application.get_all_key :ex_memcached

    # ServerState.send_data(server_state, <<"STAT pid 64\r\n">>)
    # ServerState.send_data(server_state, <<"STAT uptime 64\r\n">>)
    ServerState.send_data(server_state, <<"STAT pointer_size 64\r\n">>)
    ServerState.send_data(server_state, <<"STAT time #{now}\r\n">>)
    ServerState.send_data(server_state, <<"STAT version #{state[:vsn]}\r\n">>)
    # ServerState.send_data(server_state, <<"STAT rusage_user 64\r\n">>)
    # ServerState.send_data(server_state, <<"STAT rusage_system 64\r\n">>)
    ServerState.send_data(server_state, <<"STAT curr_items #{stats.curr_items}\r\n">>)
    # ServerState.send_data(server_state, <<"STAT total_items 64\r\n">>)
    ServerState.send_data(server_state, <<"STAT bytes #{stats.bytes}\r\n">>)
    # ServerState.send_data(server_state, <<"STAT curr_connections 64\r\n">>)
    # ServerState.send_data(server_state, <<"STAT total_connections 64\r\n">>)
    # ServerState.send_data(server_state, <<"STAT connection_structures 64\r\n">>)
    # ServerState.send_data(server_state, <<"STAT cmd_get 64\r\n">>)
    # ServerState.send_data(server_state, <<"STAT cmd_set 64\r\n">>)
    ServerState.send_data(server_state, <<"STAT get_hits #{stats.curr_items}\r\n">>)
    ServerState.send_data(server_state, <<"STAT get_misses #{stats.curr_items}\r\n">>)
    # ServerState.send_data(server_state, <<"STAT evictions 64\r\n">>)
    # ServerState.send_data(server_state, <<"STAT bytes_read 64\r\n">>)
    # ServerState.send_data(server_state, <<"STAT bytes_written 64\r\n">>)
    ServerState.send_data(server_state, <<"STAT limit_maxbytes 4297064448\r\n">>)
    # ServerState.send_data(server_state, <<"STAT threads 64\r\n">>)
    ServerState.send_data(server_state, <<"END\r\n">>)
  end
end
