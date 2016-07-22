defmodule ExMemcachedTest do
  use ExUnit.Case
  require ExMemcached.BaseDefinitions
  alias ExMemcached.BaseDefinitions, as: Bd
  require Logger

  setup_all do
    {:ok, socket} = :gen_tcp.connect('localhost', 8080, [:binary, {:packet, 0}, {:active, false}])
    {:ok, [socket: socket]}
  end

  # teardown_all meta do
  #   :gen_tcp.close(meta[:socket])
  # end

  def set key, value, flags, expiration, socket do
    :ok = :gen_tcp.send(socket, 'set #{key} #{flags} #{expiration} #{byte_size(value)}\r\n#{value}\r\n')
    {:ok, response} = :gen_tcp.recv(socket, 0)
    response
  end

    def add key, value, flags, expiration, socket do
      :ok = :gen_tcp.send(socket, 'add #{key} #{flags} #{expiration} #{byte_size(value)}\r\n#{value}\r\n')
      {:ok, response} = :gen_tcp.recv(socket, 0)
      response
    end

  def get key, socket do
    :ok = :gen_tcp.send(socket, 'get #{key}\r\n')
    Th.receive_get(socket, key, HashDict.new)
  end

  def b_set socket, key, value, flags, expiry, cas \\0 do
    body_len = 8 + byte_size(key) + byte_size(value)
    :ok = :gen_tcp.send(socket, <<Bd.protocol_binary_req, Bd.protocol_binray_cmd_set, byte_size(key)::size(16), 8, 0, 0::size(16), body_len::size(32),
      0::size(32), cas::size(64), flags::size(32), expiry::size(32) >> <> key <> value)
    {:ok, response} = :gen_tcp.recv(socket, 0)
    << Bd.protocol_binary_res, Bd.protocol_binray_cmd_set, 0::size(16), 0, 0, status::size(16), _body_len::size(32), 0::size(32), cas::size(64), _tail::binary >> = response
    {status, cas}
  end

  def b_add key, value, flags, expiry, socket do
    body_len = 8 + byte_size(key) + byte_size(value)
    :ok = :gen_tcp.send(socket, <<Bd.protocol_binary_req, Bd.protocol_binray_cmd_add, byte_size(key)::size(16), 8, 0, 0::size(16), body_len::size(32),
      0::size(32), 0::size(64), flags::size(32), expiry::size(32) >> <> key <> value)
    {:ok, response} = :gen_tcp.recv(socket, 0)
    << Bd.protocol_binary_res, Bd.protocol_binray_cmd_add, 0::size(16), 0, 0, status::size(16), _body_len::size(32), 0::size(32), cas::size(64), _tail::binary >> = response
    {status, cas}
  end

  def b_replace key, value, flags, expiry, socket do
    body_len = 8 + byte_size(key) + byte_size(value)
    :ok = :gen_tcp.send(socket, <<Bd.protocol_binary_req, Bd.protocol_binray_cmd_replace, byte_size(key)::size(16), 8, 0, 0::size(16), body_len::size(32),
      0::size(32), 0::size(64), flags::size(32), expiry::size(32) >> <> key <> value)
    {:ok, response} = :gen_tcp.recv(socket, 0)
    << Bd.protocol_binary_res, Bd.protocol_binray_cmd_replace, 0::size(16), 0, 0, status::size(16), _body_len::size(32), 0::size(32), _::size(64), _tail::binary >> = response
    status
  end

  defp get_any_extra response, needed, socket do
    len = byte_size(response)
    cond do
      len < needed ->
        {:ok, xtra} = :gen_tcp.recv(socket, needed - len)
        response <> xtra
      true ->
        response
    end
  end

  def process_get response, socket do
    response = get_any_extra response, 28, socket
    <<data::binary-size(28), _>> = response
    case data do
      << Bd.protocol_binary_res, Bd.protocol_binray_cmd_get, 0::size(16), 4, 0, Bd.protocol_binray_response_success::size(16), body_len::size(32), 0::size(32), cas::size(64), flags::size(32) >> ->
        data_len = body_len - 4
        # header + flags, response, anything else
        response = get_any_extra response, 28+data_len, socket
        << header::binary-size(28), value::binary-size(data_len) >> = response
        {value, flags, cas}
      << Bd.protocol_binary_res, Bd.protocol_binray_cmd_get, 0::size(16), 4, 0, status::size(16), _body_len::size(32), 0::size(32), _cas::size(64), _flags::size(32) >> -> {:error, status}
    end
  end

  def process_getq <<>>, result do
    result
  end

  def process_getq response, result do

    case binary_part(response, 0, 24) do
      << Bd.protocol_binary_res, Bd.protocol_binray_cmd_getq, 0::size(16), 4, 0, Bd.protocol_binray_response_success::size(16), body_len::size(32), _opaque::size(32), _cas::size(64) >> ->
        data_len = body_len - 4
        # header + flags, response, value, anything else
        <<header::binary-size(24), flags::size(32), value::binary-size(data_len), rest::binary >> = response
        process_getq rest, result ++ [{value, flags}]
      << Bd.protocol_binary_res, Bd.protocol_binray_cmd_noop, 0::size(16), 0, 0, Bd.protocol_binray_response_success::size(16), _body_len::size(32), _opaque::size(32), _cas::size(64) >> ->
        # header, anything else
        <<header::binary-size(24), rest::binary >> = response
        process_getq rest, result
    end
  end

  def b_get key, socket do
    body_len = byte_size(key)
    :ok = :gen_tcp.send(socket, <<Bd.protocol_binary_req, Bd.protocol_binray_cmd_get, body_len::size(16), 0, 0, 0::size(16), body_len::size(32),
      0::size(32), 0::size(64) >> <> key)
    {:ok, response} = :gen_tcp.recv(socket, 0)
    process_get response, socket
  end

  def b_delete key, socket do
    body_len = byte_size(key)
    :ok = :gen_tcp.send(socket, <<Bd.protocol_binary_req, Bd.protocol_binray_cmd_delete, body_len::size(16), 0, 0, 0::size(16), body_len::size(32),
      0::size(32), 0::size(64) >> <> key)
    {:ok, response} = :gen_tcp.recv(socket, 0)
    << Bd.protocol_binary_res, Bd.protocol_binray_cmd_delete, 0::size(16), 0, 0, status::size(16), _body_len::size(32), 0::size(32), _cas::size(64) >> = response
    status == Bd.protocol_binray_response_success
  end

  def b_empty key, socket do
    body_len = byte_size(key)
    :ok = :gen_tcp.send(socket, <<Bd.protocol_binary_req, Bd.protocol_binray_cmd_get, body_len::size(16), 0, 0, 0::size(16), body_len::size(32),
      0::size(32), 0::size(64) >> <> key)
    {:ok, response} = :gen_tcp.recv(socket, 0)
    << Bd.protocol_binary_res, Bd.protocol_binray_cmd_get, 0::size(16), 0, 0, status::size(16), _body_len::size(32), 0::size(32), _cas::size(64), _tail::binary >> = response
    status == Bd.protocol_binray_response_key_enoent
  end

  def b_version socket do
    :ok = :gen_tcp.send(socket, <<Bd.protocol_binary_req, Bd.protocol_binray_cmd_version, 0::size(16), 0, 0, 0::size(16), 0::size(32), 0::size(32), 0::size(64)>>)
    {:ok, response} = :gen_tcp.recv(socket, 0)
    << Bd.protocol_binary_res, Bd.protocol_binray_cmd_version, 0::size(16), 0, 0, Bd.protocol_binray_response_success::size(16), _body_len::size(32), 0::size(32), _cas::size(64) >> = binary_part(response, 0, 24)
    Bd.get_binary_tail_piece(response, 24)
  end

  def b_flush socket do
    :ok = :gen_tcp.send(socket, <<Bd.protocol_binary_req, Bd.protocol_binray_cmd_flush, 0::size(16), 4, 0, 0::size(16), 0::size(32), 0::size(32), 0::size(64), 0::size(32)>>)
    {:ok, response} = :gen_tcp.recv(socket, 0)
    << Bd.protocol_binary_res, Bd.protocol_binray_cmd_flush, 0::size(16), 0, 0, Bd.protocol_binray_response_success::size(16), _body_len::size(32), 0::size(32), _cas::size(64) >> = binary_part(response, 0, 24)
  end

  def b_noop socket do
    :ok = :gen_tcp.send(socket, <<Bd.protocol_binary_req, Bd.protocol_binray_cmd_noop, 0::size(16), 4, 0, 0::size(16), 0::size(32), 0::size(32), 0::size(64), 0::size(32)>>)
    {:ok, response} = :gen_tcp.recv(socket, 24)
    << Bd.protocol_binary_res, Bd.protocol_binray_cmd_noop, 0::size(16), 0, 0, Bd.protocol_binray_response_success::size(16), _body_len::size(32), 0::size(32), _cas::size(64) >> = binary_part(response, 0, 24)
  end

  def b_get_multi keys, socket do
    Enum.with_index(keys) |> Enum.each(fn({key, index}) ->
      body_len = byte_size(key)
      :ok = :gen_tcp.send(socket, <<Bd.protocol_binary_req, Bd.protocol_binray_cmd_getq, body_len::size(16), 0, 0, 0::size(16), body_len::size(32),
        index::size(32), 0::size(64) >> <> key)
    end)
    # send noop
    :ok = :gen_tcp.send(socket, <<Bd.protocol_binary_req, Bd.protocol_binray_cmd_noop, 0::size(16), 4, 0, 0::size(16), 0::size(32), 0::size(32), 0::size(64), 0::size(32)>>)

    case :gen_tcp.recv(socket, 0, 2000) do
      {:ok, response} ->
        process_getq response, []
      res ->
        res
    end
  end

  def b_incr key, socket, count \\ 1, initial \\ 0 do
    case b_incr_cas key, socket, count, initial do
      {ans, cas} -> ans
      :invalid -> :invalid
      :bad_value -> :bad_value
    end
  end

  def b_incr_cas key, socket, count \\ 1, initial \\ 0 do
    key_len = byte_size(key)
    :ok = :gen_tcp.send(socket, <<Bd.protocol_binary_req, Bd.protocol_binray_cmd_increment, key_len::size(16), 20, 0, 0::size(16), 20+key_len::size(32), 0::size(32), 0::size(64),
      count::unsigned-size(64), initial::size(64), 0::size(32) >> <> key)
    {:ok, response} = :gen_tcp.recv(socket, 24)
    << Bd.protocol_binary_res, Bd.protocol_binray_cmd_increment, 0::size(16), 0, 0, status::size(16), body_len::size(32),
      0::size(32), cas::size(64) >> = response
    case status do
      Bd.protocol_binray_response_success ->
        {:ok, response} = :gen_tcp.recv(socket, 8)
        << ans::unsigned-size(64)>> = response
        {ans, cas}
      Bd.protocol_binray_response_einval ->
        {:ok, response} = :gen_tcp.recv(socket, body_len)
        :invalid
      Bd.protocol_binray_response_delta_badval ->
        {:ok, response} = :gen_tcp.recv(socket, body_len)
        :bad_value
    end
  end

  def b_decr key, socket, count \\ 1, initial \\ 0 do
    key_len = byte_size(key)
    :ok = :gen_tcp.send(socket, <<Bd.protocol_binary_req, Bd.protocol_binray_cmd_decrement, key_len::size(16), 20, 0, 0::size(16), 20+key_len::size(32), 0::size(32), 0::size(64),
      count::unsigned-size(64), initial::size(64), 0::size(32) >> <> key)
    {:ok, response} = :gen_tcp.recv(socket, 24)
    << Bd.protocol_binary_res, Bd.protocol_binray_cmd_decrement, 0::size(16), 0, 0, status::size(16), _body_len::size(32),
      0::size(32), _cas::size(64) >> = response
    case status do
      Bd.protocol_binray_response_success ->
        {:ok, response} = :gen_tcp.recv(socket, 8)
        << ans::unsigned-size(64)>> = response
        ans
      Bd.protocol_binray_response_einval ->
        :invalid
    end
  end

  def stats socket do
    :ok = :gen_tcp.send(socket, 'stats\r\n')
    Th.receive_stats(socket, HashDict.new)
  end

  def stats kind, socket do
    :ok = :gen_tcp.send(socket, 'stats #{kind}\r\n')
    Th.receive_stats(socket, HashDict.new)
  end

  test "64bit.t", meta do
    # part of 64bit.t
    values = stats meta[:socket]
    assert(Dict.get(values, "pointer_size") == "64")
    assert(Dict.get(values, "limit_maxbytes") == "4297064448")
    values = stats "slabs", meta[:socket]
    assert(Dict.get(values, "total_malloced") == "4294967328")
    assert(Dict.get(values, "active_slabs") == "0")
  end

  test "binary-get.t", meta do
    # test in binary-get.t
    Enum.with_index(["mooo\0", "mumble\0\0\0\0\r\rblarg", "\0", "\r"])
      |> Enum.each(fn ({item, idx}) ->
        key = "foo#{idx}"
        response = set key, item, 0, 0, meta[:socket]
        assert(response == "STORED\r\n")
        values = get key, meta[:socket]
        assert Dict.get(values, key) == item
      end)
  end

  test "binary.t", meta do
    values = stats "detail on", meta[:socket]
    assert Dict.size(values) == 0

    # test version
    assert "0.0.2" == b_version meta[:socket]

    # bug 71
    stats meta[:socket]
    b_flush meta[:socket]
    stats meta[:socket]

    # flushing
    b_flush meta[:socket]

    # noop
    b_noop meta[:socket]

    # simple set/get
    b_set meta[:socket], "x", "somevale", 5, 19
    Logger.info "<<<<<<<< here >>>>>>>>>>"
    {"somevale", 5, _} = b_get "x", meta[:socket]

    # delete
    assert b_delete "x", meta[:socket]
    assert b_empty "x", meta[:socket]

    {Bd.protocol_binray_response_success, cas} = b_set meta[:socket], "x", "somevalex", 5, 19
    {"somevalex", 5, _} = b_get "x", meta[:socket]
    {Bd.protocol_binray_response_success, cas} = b_set meta[:socket], "y", "somevaley", 5, 17
    {"somevaley", 5, _} = b_get "y", meta[:socket]
    b_flush meta[:socket]
    assert b_empty "x", meta[:socket]
    assert b_empty "y", meta[:socket]

    # add
    assert b_empty "i", meta[:socket]
    {Bd.protocol_binray_response_success, _} = b_add "i", "ex", 5, 10, meta[:socket]
    {"ex", 5, _} = b_get "i", meta[:socket]
    {Bd.protocol_binray_response_key_eexists, _} = b_add "i", "ex", 5, 10, meta[:socket]
    {"ex", 5, _} = b_get "i", meta[:socket]

    # too big
    assert b_empty "toobig", meta[:socket]
    {Bd.protocol_binray_response_success, cas} == b_set meta[:socket], "toobig", "not too big", 10, 10
    {"not too big", 10, _} = b_get "toobig", meta[:socket]
    s = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
    s = Enum.reduce 1..10484, s, fn(_, acc) -> acc <> s end
    s = s <> "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
    # s has 1024*1024 + 1 long string
    {Bd.protocol_binray_response_e2big, cas} == b_set meta[:socket], "toobig", s, 10, 10
    assert b_empty "toobig", meta[:socket]

    # Replace
    assert b_empty "j", meta[:socket]
    assert Bd.protocol_binray_response_key_enoent == b_replace "j", "ex", 19, 5, meta[:socket]
    assert b_empty "j", meta[:socket]

    {Bd.protocol_binray_response_success, _} = b_add "j", "ex2", 14, 5, meta[:socket]
    {"ex2", 14, _} = b_get "j", meta[:socket]
    assert Bd.protocol_binray_response_success == b_replace "j", "ex3", 24, 5, meta[:socket]
    {"ex3", 24, _} = b_get "j", meta[:socket]

    # Multiget
    # my $rv = $mc->get_multi(qw(xx wye zed));

    {Bd.protocol_binray_response_success, _} = b_add "xx", "ex", 1, 5, meta[:socket]
    {Bd.protocol_binray_response_success, _} = b_add "wye", "why", 2, 5, meta[:socket]
    [{"ex", 1}, {"why", 2}] = b_get_multi ["xx", "wye", "zed"], meta[:socket]

    # test increment
    b_flush meta[:socket]
    assert 0 == b_incr "x", meta[:socket]
    assert 1 == b_incr "x", meta[:socket]
    assert 212 == b_incr "x", meta[:socket], 211
    assert 8589934804 == b_incr "x", meta[:socket], Float.floor(:math.pow(2, 33))

    # Issue 48 - incrementing plain text.
    b_set meta[:socket], "issue48", "text", 0, 0
    assert :bad_value == b_incr "issue48", meta[:socket]
    {"text", 0, _} = b_get "issue48", meta[:socket]

    assert :bad_value == b_incr "issue48", meta[:socket]
    {"text", 0, _} = b_get "issue48", meta[:socket]

    # Issue 320 - incr/decr wrong length for initial value
    b_flush meta[:socket]
    assert 1 == b_incr "issue320", meta[:socket], 1, 1

    # Test decrement
    b_flush meta[:socket]
    b_incr "x", meta[:socket], 0xffffffff, 5
    assert 4 == b_decr "x", meta[:socket]
    assert 0 == b_decr "x", meta[:socket], 211

    # bug220
    {_, cas} = b_set meta[:socket], "bug220", "100", 0, 0
    {1099, cas2} = b_incr_cas "bug220", meta[:socket], 999
    assert cas != cas2
    {<< val::size(64) >>, _, cas3} = b_get "bug220", meta[:socket]
    assert val == 1099
    assert cas2 == cas3

    {2098, cas2} = b_incr_cas "bug220", meta[:socket], 999
    assert cas != cas2
    {<< val::size(64) >>, _, cas3} = b_get "bug220", meta[:socket]
    assert val == 2098
    assert cas2 == cas3

    # bug21
    b_add "bug21", "9223372036854775807", 0, 0, meta[:socket]
    assert 9223372036854775808 == b_incr "bug21", meta[:socket]
    assert 9223372036854775809 == b_incr "bug21", meta[:socket]
    assert 9223372036854775808 == b_decr "bug21", meta[:socket]

    # diag "CAS";
    b_flush meta[:socket]
    {Bd.protocol_binray_response_key_enoent, 0} = b_set meta[:socket], "x", "bad value", 15, 5, 0x7FFFFFF

    {_, cas} = b_add "x", "original value", 5, 19, meta[:socket]
    {"original value", 5, ^cas} = b_get "x", meta[:socket]

    {Bd.protocol_binray_response_key_eexists, _} = b_set meta[:socket], "x", "bad value", 15, 5, cas+1

    {Bd.protocol_binray_response_success, new_cas} = b_set meta[:socket], "x", "new value", 19, 5, cas
    {"new value", 19, ^new_cas} = b_get "x", meta[:socket]

    {Bd.protocol_binray_response_key_eexists, _} = b_set meta[:socket], "x", "replay value", 19, 5, cas

    # Touch commands

  end
end
