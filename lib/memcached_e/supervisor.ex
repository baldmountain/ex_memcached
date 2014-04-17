defmodule MemcachedE.Supervisor do
  import Supervisor.Spec

  def start_link do
    GenServer.start_link(__MODULE__, [])
  end

  def init([]) do
    children = [
      # Define workers and child supervisors to be supervised
      worker(MemcachedE.Worker, [])
    ]

    Supervisor.start_link(children, strategy: :one_for_one)
  end
end
