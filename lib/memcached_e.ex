defmodule MemcachedE do
  use Application

  # See http://elixir-lang.org/docs/stable/Application.Behaviour.html
  # for more information on OTP Applications
  def start(_type, []) do
    import Supervisor.Spec

    children = [
      worker(MemcachedE.Supervisor, [])
    ]

    Supervisor.start_link(children, strategy: :one_for_one)
  end

  def stop do
  end

  def put key, value do
    :gen_server.cast(:cache, {:put, key, value})
  end

  def get key do
    :gen_server.call(:cache, {:get, key})
  end
end
