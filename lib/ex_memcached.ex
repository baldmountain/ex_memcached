defmodule ExMemcached do
  use Application
  require Logger

  # See http://elixir-lang.org/docs/stable/Application.Behaviour.html
  # for more information on OTP Applications
  def start(_type, []) do
    import Supervisor.Spec
    port = Application.get_env(:ex_memcached, :listen_port)
    Logger.info "listening on port: #{port}"
    {:ok, _} = :ranch.start_listener(:tcp_server, 1, :ranch_tcp, [{:port, port}], ExMemcached.Server, [])

    children = [
      worker(ExMemcached.Supervisor, [])
    ]

    Supervisor.start_link(children, strategy: :one_for_one)

  end

  def stop do
  end

  # Wrappers around the calls to worker.ex
  def set key, value, flags, exptime, cas do
    GenServer.call(:cache, {:set, key, value, flags, exptime, cas})
  end

  def add key, value, flags, exptime do
    GenServer.call(:cache, {:add, key, value, flags, exptime})
  end

  def replace key, value, flags, exptime do
    GenServer.call(:cache, {:replace, key, value, flags, exptime})
  end

  def get key do
    GenServer.call(:cache, {:get, key})
  end

  def gat key, exptime do
    GenServer.call(:cache, {:gat, key, exptime})
  end

  def delete key do
    GenServer.call(:cache, {:delete, key})
  end

  def touch key, expiration do
    GenServer.call(:cache, {:touch, key, expiration})
  end

  def incr key, count, initial, expiration  do
    GenServer.call(:cache, {:incr, key, count, initial, expiration})
  end

  def decr key, count, initial, expiration  do
    GenServer.call(:cache, {:decr, key, count, initial, expiration})
  end

  def flush expiration do
    GenServer.call(:cache, {:flush, expiration})
  end

  def append key, value, flags, exptime do
    GenServer.call(:cache, {:append, key, value, flags, exptime})
  end

  def prepend key, value, flags, exptime do
    GenServer.call(:cache, {:prepend, key, value, flags, exptime})
  end

  def cas key, value, flags, exptime, cas do
    GenServer.call(:cache, {:cas, key, value, flags, exptime, cas})
  end

  def stats do
    GenServer.call(:cache, {:stats})
  end
end
