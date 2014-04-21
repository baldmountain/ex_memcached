defmodule MemcachedE do
  use Application

  # See http://elixir-lang.org/docs/stable/Application.Behaviour.html
  # for more information on OTP Applications
  def start(_type, []) do
    import Supervisor.Spec

    {:ok, _} = :ranch.start_listener(:tcp_server, 1, :ranch_tcp, [{:port, 8080}], MemcachedE.Server, [])

    children = [
      worker(MemcachedE.Supervisor, [])
    ]

    Supervisor.start_link(children, strategy: :one_for_one)

  end

  def stop do
  end

  def set key, value, flags, exptime do
    :gen_server.call(:cache, {:set, key, value, flags, exptime})
  end

  def add key, value, flags, exptime do
    :gen_server.call(:cache, {:add, key, value, flags, exptime})
  end

  def replace key, value, flags, exptime do
    :gen_server.call(:cache, {:replace, key, value, flags, exptime})
  end

  def get key do
    :gen_server.call(:cache, {:get, key})
  end

  def delete key do
    :gen_server.call(:cache, {:delete, key})
  end

  def incr key, count, initial, expiration  do
    :gen_server.call(:cache, {:incr, key, count, initial, expiration})
  end

  def decr key, count do
    :gen_server.call(:cache, {:decr, key, count})
  end
end
