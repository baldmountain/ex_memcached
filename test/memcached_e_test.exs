defmodule MemcachedETest do
  use ExUnit.Case
  require MemcachedE.BaseDefinitions
  alias MemcachedE.BaseDefinitions, as: Bd

  setup_all do
    {:ok, socket} = :gen_tcp.connect('localhost', 8080, [:binary, {:packet, 0}, {:active, false}])
    {:ok, [socket: socket]}
  end

  teardown_all meta do
    :gen_tcp.close(meta[:socket])
  end

  def set key, value, flags, expiration, socket do
    :ok = :gen_tcp.send(socket, 'set #{key} #{flags} #{expiration} #{size(value)}\r\n#{value}\r\n')
    {:ok, response} = :gen_tcp.recv(socket, 0)
    response
  end

    def add key, value, flags, expiration, socket do
      :ok = :gen_tcp.send(socket, 'add #{key} #{flags} #{expiration} #{size(value)}\r\n#{value}\r\n')
      {:ok, response} = :gen_tcp.recv(socket, 0)
      response
    end

  def get key, socket do
    :ok = :gen_tcp.send(socket, 'get #{key}\r\n')
    Th.receive_get(socket, key, HashDict.new)
  end

  def b_set key, value, flags, expiry, socket do
    body_len = 8 + size(key) + size(value)
    :ok = :gen_tcp.send(socket, <<Bd.protocol_binary_req, Bd.protocol_binray_cmd_set, size(key)::size(16), 8, 0, 0::size(16), body_len::size(32),
      0::size(32), 0::size(64), flags::size(32), expiry::size(32) >> <> key <> value)
    {:ok, response} = :gen_tcp.recv(socket, 0)
    << Bd.protocol_binary_res, Bd.protocol_binray_cmd_set, 0::size(16), 0, 0, status::size(16), 0::size(32), 0::size(32), _::size(64) >> = response
    status
  end

  def b_add key, value, flags, expiry, socket do
    body_len = 8 + size(key) + size(value)
    :ok = :gen_tcp.send(socket, <<Bd.protocol_binary_req, Bd.protocol_binray_cmd_add, size(key)::size(16), 8, 0, 0::size(16), body_len::size(32),
      0::size(32), 0::size(64), flags::size(32), expiry::size(32) >> <> key <> value)
    {:ok, response} = :gen_tcp.recv(socket, 0)
    << Bd.protocol_binary_res, Bd.protocol_binray_cmd_add, 0::size(16), 0, 0, status::size(16), 0::size(32), 0::size(32), _::size(64) >> = response
    status
  end

  def b_replace key, value, flags, expiry, socket do
    body_len = 8 + size(key) + size(value)
    :ok = :gen_tcp.send(socket, <<Bd.protocol_binary_req, Bd.protocol_binray_cmd_replace, size(key)::size(16), 8, 0, 0::size(16), body_len::size(32),
      0::size(32), 0::size(64), flags::size(32), expiry::size(32) >> <> key <> value)
    {:ok, response} = :gen_tcp.recv(socket, 0)
    << Bd.protocol_binary_res, Bd.protocol_binray_cmd_replace, 0::size(16), 0, 0, status::size(16), 0::size(32), 0::size(32), _::size(64) >> = response
    status
  end

  def b_get key, socket do
    body_len = size(key)
    :ok = :gen_tcp.send(socket, <<Bd.protocol_binary_req, Bd.protocol_binray_cmd_get, body_len::size(16), 0, 0, 0::size(16), body_len::size(32),
      0::size(32), 0::size(64) >> <> key)
    {:ok, response} = :gen_tcp.recv(socket, 0)
    case String.slice(response, 0..27) do
      << Bd.protocol_binary_res, Bd.protocol_binray_cmd_get, 0::size(16), 4, 0, Bd.protocol_binray_response_success::size(16), _body_len::size(32), 0::size(32), _cas::size(64), flags::size(32) >> -> {String.slice(response, 28..-1), flags}
      << Bd.protocol_binary_res, Bd.protocol_binray_cmd_get, 0::size(16), 4, 0, status::size(16), _body_len::size(32), 0::size(32), _cas::size(64), flags::size(32) >> -> {:error, status}
    end
  end

  def b_delete key, socket do
    body_len = size(key)
    :ok = :gen_tcp.send(socket, <<Bd.protocol_binary_req, Bd.protocol_binray_cmd_delete, body_len::size(16), 0, 0, 0::size(16), body_len::size(32),
      0::size(32), 0::size(64) >> <> key)
    {:ok, response} = :gen_tcp.recv(socket, 0)
    << Bd.protocol_binary_res, Bd.protocol_binray_cmd_delete, 0::size(16), 0, 0, status::size(16), _body_len::size(32), 0::size(32), _cas::size(64) >> = response
    status == Bd.protocol_binray_response_success
  end

  def b_empty key, socket do
    body_len = size(key)
    :ok = :gen_tcp.send(socket, <<Bd.protocol_binary_req, Bd.protocol_binray_cmd_get, body_len::size(16), 0, 0, 0::size(16), body_len::size(32),
      0::size(32), 0::size(64) >> <> key)
    {:ok, response} = :gen_tcp.recv(socket, 0)
    << Bd.protocol_binary_res, Bd.protocol_binray_cmd_get, 0::size(16), 0, 0, status::size(16), _body_len::size(32), 0::size(32), _cas::size(64) >> = response
    status == Bd.protocol_binray_response_key_enoent
  end

  def b_version socket do
    :ok = :gen_tcp.send(socket, <<Bd.protocol_binary_req, Bd.protocol_binray_cmd_version, 0::size(16), 0, 0, 0::size(16), 0::size(32), 0::size(32), 0::size(64)>>)
    {:ok, response} = :gen_tcp.recv(socket, 0)
    << Bd.protocol_binary_res, Bd.protocol_binray_cmd_version, 0::size(16), 0, 0, Bd.protocol_binray_response_success::size(16), _body_len::size(32), 0::size(32), _cas::size(64) >> = String.slice(response, 0..23)
    String.slice(response, 24..-1)
  end

  def b_flush socket do
    :ok = :gen_tcp.send(socket, <<Bd.protocol_binary_req, Bd.protocol_binray_cmd_flush, 0::size(16), 4, 0, 0::size(16), 0::size(32), 0::size(32), 0::size(64), 0::size(32)>>)
    {:ok, response} = :gen_tcp.recv(socket, 0)
    << Bd.protocol_binary_res, Bd.protocol_binray_cmd_flush, 0::size(16), 0, 0, Bd.protocol_binray_response_success::size(16), _body_len::size(32), 0::size(32), _cas::size(64) >> = String.slice(response, 0..23)
  end

  def b_noop socket do
    :ok = :gen_tcp.send(socket, <<Bd.protocol_binary_req, Bd.protocol_binray_cmd_noop, 0::size(16), 4, 0, 0::size(16), 0::size(32), 0::size(32), 0::size(64), 0::size(32)>>)
    {:ok, response} = :gen_tcp.recv(socket, 0)
    << Bd.protocol_binary_res, Bd.protocol_binray_cmd_noop, 0::size(16), 0, 0, Bd.protocol_binray_response_success::size(16), _body_len::size(32), 0::size(32), _cas::size(64) >> = String.slice(response, 0..23)
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
      |> Enum.each fn ({item, idx}) ->
        key = "foo#{idx}"
        response = set key, item, 0, 0, meta[:socket]
        assert(response == "STORED\r\n")
        values = get key, meta[:socket]
        assert Dict.get(values, key) == item
      end
  end

  test "binary.t", meta do
    values = stats "detail on", meta[:socket]
    assert Dict.size(values) == 0

    # test version
    assert "0.0.1" == b_version meta[:socket]

    # bug 71
    stats meta[:socket]
    b_flush meta[:socket]
    stats meta[:socket]

    # flushing
    b_flush meta[:socket]

    # noop
    b_noop meta[:socket]

    # simple set/get
    b_set "x", "somevale", 5, 19, meta[:socket]
    {"somevale", 5} = b_get "x", meta[:socket]

    # delete
    assert b_delete "x", meta[:socket]
    assert b_empty "x", meta[:socket]

    assert Bd.protocol_binray_response_success == b_set "x", "somevalex", 5, 19, meta[:socket]
    {"somevalex", 5} = b_get "x", meta[:socket]
    assert Bd.protocol_binray_response_success == b_set "y", "somevaley", 5, 17, meta[:socket]
    {"somevaley", 5} = b_get "y", meta[:socket]
    b_flush meta[:socket]
    assert b_empty "x", meta[:socket]
    assert b_empty "y", meta[:socket]

    # add
    assert b_empty "i", meta[:socket]
    assert Bd.protocol_binray_response_success == b_add "i", "ex", 5, 10, meta[:socket]
    {"ex", 5} = b_get "i", meta[:socket]
    assert Bd.protocol_binray_response_key_eexists == b_add "i", "ex", 5, 10, meta[:socket]
    {"ex", 5} = b_get "i", meta[:socket]

    # too big
    assert b_empty "toobig", meta[:socket]
    assert Bd.protocol_binray_response_success == b_set "toobig", "not too big", 10, 10, meta[:socket]
    {"not too big", 10} = b_get "toobig", meta[:socket]
    s = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
    s = Enum.reduce 1..10484, s, fn(i, acc) -> acc <> s end
    s = s <> "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
    # s has 1024*1024 + 1 long string
    assert Bd.protocol_binray_response_e2big == b_set "toobig", s, 10, 10, meta[:socket]
    assert b_empty "toobig", meta[:socket]

    # Replace
    assert b_empty "j", meta[:socket]
    assert Bd.protocol_binray_response_key_enoent == b_replace "j", "ex", 19, 5, meta[:socket]
    assert b_empty "j", meta[:socket]

    assert Bd.protocol_binray_response_success == b_add "j", "ex2", 14, 5, meta[:socket]
    {"ex2", 14} = b_get "j", meta[:socket]
    assert Bd.protocol_binray_response_success == b_replace "j", "ex3", 24, 5, meta[:socket]
    {"ex3", 24} = b_get "j", meta[:socket]

  end

end
