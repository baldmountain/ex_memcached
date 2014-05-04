defmodule ExMemcached.ServerState do
  defstruct socket: nil, transport: nil, existing_data: "", stored_responses: << >>

  def close_transport(server_state = %ExMemcached.ServerState{})do
    :timer.sleep(500)
    server_state[:trasnport].close(server_state[:socket])
  end
end
