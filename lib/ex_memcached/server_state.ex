defmodule ExMemcached.ServerState do
  defstruct socket: nil, transport: nil, existing_data: "", stored_responses: << >>

  def close_transport(server_state = %ExMemcached.ServerState{}) do
    :timer.sleep(500)
    server_state.transport.close(server_state.socket)
  end

  def send_data(server_state = %ExMemcached.ServerState{}, data) do
    server_state.transport.send(server_state.socket, data)
    server_state
  end
end
