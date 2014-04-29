defmodule MemcachedE do
  use Application
  require Lager

  # See http://elixir-lang.org/docs/stable/Application.Behaviour.html
  # for more information on OTP Applications
  def start(_type, []) do
    import Supervisor.Spec
    port = Application.get_env(:memcached_e, :listen_port)
    Lager.info "listening on port: #{port}"
    {:ok, _} = :ranch.start_listener(:tcp_server, 1, :ranch_tcp, [{:port, port}], MemcachedE.Server, [])

    children = [
      worker(MemcachedE.Supervisor, [])
    ]

    Supervisor.start_link(children, strategy: :one_for_one)

  end

  def stop do
  end

  def set key, value, flags, exptime, cas do
    :gen_server.call(:cache, {:set, key, value, flags, exptime, cas})
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

  def touch key, expiration do
    :gen_server.call(:cache, {:touch, key, expiration})
  end

  def incr key, count, initial, expiration  do
    :gen_server.call(:cache, {:incr, key, count, initial, expiration})
  end

  def decr key, count, initial, expiration  do
    :gen_server.call(:cache, {:decr, key, count, initial, expiration})
  end

  def flush expiration do
    :gen_server.call(:cache, {:flush, expiration})
  end

  def append key, value, flags, exptime do
    :gen_server.call(:cache, {:append, key, value, flags, exptime})
  end

  def prepend key, value, flags, exptime do
    :gen_server.call(:cache, {:prepend, key, value, flags, exptime})
  end

  def cas key, value, flags, exptime, cas do
    :gen_server.call(:cache, {:cas, key, value, flags, exptime, cas})
  end
end
