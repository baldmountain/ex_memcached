defmodule ExMemcached.ServerState do
  require Logger
  defstruct socket: nil, transport: nil, existing_data: "", stored_responses: << >>

  def close_transport(server_state = %ExMemcached.ServerState{}) do
    Logger.debug("Closing: #{Exception.format_stacktrace()}")
    :timer.sleep(500)
    server_state.transport.close(server_state.socket)
    exit("transport closed")
  end

  def send_data(server_state = %ExMemcached.ServerState{}, data) do
    # Logger.debug "send_data #{inspect data}, len: #{byte_size(data)}"
    server_state.transport.send(server_state.socket, data)
    server_state
  end
end
