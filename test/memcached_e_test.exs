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

  def set key, value, socket do
    :ok = :gen_tcp.send(socket, 'set #{key} 0 0 #{size(value)}\r\n#{value}\r\n')
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
    << Bd.protocol_binary_res, Bd.protocol_binray_cmd_set, 0::size(16), 0, 0, Bd.protocol_binray_response_success::size(16), 0::size(32), 0::size(32), _::size(64) >> = response
    :ok
  end

  def b_get key, socket do
    body_len = size(key)
    :ok = :gen_tcp.send(socket, <<Bd.protocol_binary_req, Bd.protocol_binray_cmd_get, body_len::size(16), 0, 0, 0::size(16), body_len::size(32),
      0::size(32), 0::size(64) >> <> key)
    {:ok, response} = :gen_tcp.recv(socket, 0)
    << Bd.protocol_binary_res, Bd.protocol_binray_cmd_get, 0::size(16), 4, 0, Bd.protocol_binray_response_success::size(16), _body_len::size(32), 0::size(32), _cas::size(64), flags::size(32) >> = String.slice(response, 0..27)
    {String.slice(response, 28..-1), flags}
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
        response = set key, item, meta[:socket]
        assert(response == "STORED\r\n")
        values = get key, meta[:socket]
        assert Dict.get(values, key) == item
      end
  end

  test "binary.t", meta do
    values = stats "detail on", meta[:socket]
    assert Dict.size(values) == 0

    b_set "foo", "bar", 0, 0, meta[:socket]
    {"bar", 0} = b_get "foo", meta[:socket]

    assert "0.0.1" == b_version meta[:socket]

    stats meta[:socket]
    b_flush meta[:socket]
    stats meta[:socket]

  end

end
