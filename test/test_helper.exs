ExUnit.start

defmodule Th do
  def receive_stats(socket, values) do
    case :gen_tcp.recv(socket, 0) do
      {:ok, data} ->
        case Enum.reduce(String.split(data, "\r\n"), {false, values}, fn(item, acc) ->
            case acc do
              {false, values} ->
                case String.split(item, " ") do
                  ["STAT", name, value] ->
                    values = Dict.put(values, name, value)
                    {false, values}
                  ["END"] ->
                    {true, values}
                  _ ->
                    {false, values}
                end
              {true, values} ->
                {true, values}
            end
          end) do
          {true, values} ->
            values
          {false, values} ->
             receive_stats(socket, values)
        end
      {:error, :closed} -> values
    end
  end

  def receive_get(socket, key, values) do
    case :gen_tcp.recv(socket, 0) do
      {:ok, data} ->
        case Enum.reduce(String.split(data, "\r\n"), {false, values}, fn(item, acc) ->
            case acc do
              {false, values} ->
                case String.split(item, " ") do
                  ["VALUE", ^key, _flags, _bytes] ->
                    {false, values}
                  ["END"] ->
                    {true, values}
                  [value] ->
                    values = Dict.put(values, key, value)
                    {false, values}
                end
              {true, values} ->
                {true, values}
            end
          end) do
          {true, values} ->
            values
          {false, values} ->
             receive_get(socket, key, values)
        end
      {:error, :closed} -> values
    end
  end

end
