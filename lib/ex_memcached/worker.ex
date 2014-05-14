defmodule ExMemcached.Worker do
  use GenServer
  require Lager

  defmodule WorkerState do
    defstruct misses: 0, hits: 0, allocated: 0, current_cas: 1

    def next_cas(ws = %WorkerState{}) do
      %WorkerState{ws | current_cas: ws.current_cas + 1}
    end

    def hit(ws = %WorkerState{}) do
      %WorkerState{ws | hits: ws.hits + 1}
    end

    def miss(ws = %WorkerState{}) do
      %WorkerState{ws | misses: ws.misses + 1}
    end
  end

  def start_link do
      GenServer.start_link(__MODULE__, [], name: :cache)
  end

  def init([]) do
    Lager.info "Worker init"
    { :ok, {HashDict.new, %WorkerState{}} }
  end

  def handle_call({:get, key} , _from, {data, work_state}) do
    case Dict.get(data, key) do
      nil ->
        { :reply, :not_found, {data, WorkerState.miss(work_state)} }
      {value, timestamp, flags, exptime, cas} ->
        case check_expiration(value, timestamp, exptime) do
          nil ->
            { :reply, :not_found, {HashDict.delete(data, key), WorkerState.miss(work_state)} }
          value ->
            { :reply, {value, flags, cas}, {data, WorkerState.hit(work_state)} }
        end
    end
  end

  def handle_call({:set, key, value, flags, exptime, cas}, _from, {data, work_state}) do
    case cas do
      0 ->
        data = Dict.put(data, key, {value, generate_expire_time(exptime), flags, exptime, work_state.current_cas})
        { :reply, {:stored, work_state.current_cas}, {data, WorkerState.next_cas(work_state)} }
      _ ->
        case Dict.get(data, key) do
          nil ->
            { :reply, :not_found, {data, work_state} }
          {_, _, _, _, ^cas} ->
            data = Dict.put(data, key, {value, generate_expire_time(exptime), flags, exptime, work_state.current_cas})
            { :reply, {:stored, work_state.current_cas}, {data, WorkerState.next_cas(work_state)} }
          {_, _, _, _, _} ->
            { :reply, :exists, {data, work_state} }
        end
    end
  end

  def handle_call({:add, key, value, flags, expirary}, _from, {data, work_state}) do
    case Dict.get(data, key) do
      nil ->
        data = Dict.put(data, key, {value, generate_expire_time(expirary), flags, expirary, work_state.current_cas})
        { :reply, {:stored, work_state.current_cas}, {data, WorkerState.next_cas(work_state)} }
      {old_value, timestamp, _, exptime, _} ->
        case check_expiration(old_value, timestamp, exptime) do
          nil ->
            data = Dict.put(data, key, {value, generate_expire_time(expirary), flags, expirary, work_state.current_cas})
            { :reply, {:stored, work_state.current_cas}, {data, WorkerState.next_cas(work_state)} }
          _ ->
            { :reply, :not_stored, {data, work_state} }
        end
    end
  end

  def handle_call({:replace, key, value, flags, expirary}, _from, {data, work_state}) do
    case Dict.get(data, key) do
      nil ->
        { :reply, :not_stored, {data, work_state} }
      {old_value, timestamp, _, exptime, _} ->
        case check_expiration(old_value, timestamp, exptime) do
          nil ->
            { :reply, :not_stored, {data, work_state} }
          _ ->
            data = Dict.put(data, key, {value, generate_expire_time(expirary), flags, expirary, work_state.current_cas})
            { :reply, {:stored, work_state.current_cas}, {data, WorkerState.next_cas(work_state)} }
        end
    end
  end

  def handle_call({:append, key, value, _flags, _exptime}, _from, {data, work_state}) do
    case Dict.get(data, key) do
      nil ->
        { :reply, :not_stored, {data, work_state} }
      {evalue, timestamp, eflags, eexptime, _} ->
        data = Dict.put(data, key, {evalue <> value, timestamp, eflags, eexptime, work_state.current_cas})
        { :reply, {:stored, work_state.current_cas}, {data, WorkerState.next_cas(work_state)} }
    end
  end

  def handle_call({:prepend, key, value, _flags, _exptime}, _from, {data, work_state}) do
    case Dict.get(data, key) do
      nil ->
        { :reply, :not_stored, {data, work_state} }
      {evalue, timestamp, eflags, eexptime, _} ->
        data = Dict.put(data, key, {value <> evalue, timestamp, eflags, eexptime, work_state.current_cas})
        { :reply, {:stored, work_state.current_cas}, {data, WorkerState.next_cas(work_state)} }
    end
  end

  def handle_call({:cas, key, value, flags, exptime, cas}, _from, {data, work_state}) do
    case Dict.get(data, key) do
      {_, _, _, _, ^cas} ->
        data = Dict.put(data, key, {value, generate_expire_time(exptime), flags, exptime, cas})
        { :reply, {:stored, work_state.current_cas}, {data, work_state} }
      {_, _, _, _, _} ->
        { :reply, :exists, {data, work_state} }
      _ ->
        { :reply, :not_found, {data, work_state} }
    end
  end

  def handle_call({:delete, key}, _from, {data, work_state}) do
    case Dict.get(data, key) do
      {value, timestamp, _, exptime, _} ->
        case check_expiration(value, timestamp, exptime) do
          nil ->
            { :reply, :not_found, {HashDict.delete(data, key), work_state} }
          _value ->
            data = Dict.delete(data, key)
            { :reply, :deleted, {data, work_state} }
        end
      _ ->
        { :reply, :not_found, {data, work_state} }
    end
  end

  def handle_call({:touch, key, expiration}, _from, {data, work_state}) do
    case Dict.get(data, key) do
      {value, timestamp, flags, exptime, _} ->
        case check_expiration(value, timestamp, exptime) do
          nil ->
            { :reply, :not_found, {HashDict.delete(data, key), work_state} }
          _value ->
            data = Dict.put(data, key, {value, generate_expire_time(expiration), flags, expiration, work_state.current_cas})
            { :reply, :touched, {data, WorkerState.next_cas(work_state)} }
        end
      _ ->
        { :reply, :not_found, {data, work_state} }
    end
  end

  def handle_call({:gat, key, expiration} , _from, {data, work_state}) do
    case Dict.get(data, key) do
      nil ->
        { :reply, :not_found, {data, WorkerState.miss(work_state)} }
      {value, timestamp, flags, exptime, cas} ->
        case check_expiration(value, timestamp, exptime) do
          nil ->
            { :reply, :not_found, {HashDict.delete(data, key), WorkerState.miss(work_state)} }
          value ->
            data = Dict.put(data, key, {value, generate_expire_time(expiration), flags, expiration, work_state.current_cas})
            { :reply, {value, flags, cas}, {data, WorkerState.next_cas(WorkerState.hit(work_state))} }
        end
    end
  end

  def handle_call({:incr, key, count, intial, expiration}, _from, {data, work_state}) do
    case Dict.get(data, key) do
      {value, timestamp, flags, exptime, _} ->
        case check_expiration(value, timestamp, exptime) do
          nil when expiration == 0xffffffff ->
            { :reply, :not_found, {HashDict.delete(data, key), work_state} }
          nil ->
            data = Dict.put(data, key, {intial, generate_expire_time(expiration), 0, expiration, work_state.current_cas})
            { :reply, {intial, work_state.current_cas}, {data, WorkerState.next_cas(work_state)} }
          value when is_integer(value) ->
            value = value + count
            if value > 0xffffffffffffffff, do: value = value - 0x10000000000000000
            data = Dict.put(data, key, {value, generate_expire_time(expiration), flags, exptime, work_state.current_cas})
            { :reply, {value, work_state.current_cas}, {data, WorkerState.next_cas(work_state)} }
          value when is_binary(value) ->
            try do
              value = binary_to_integer(value) + count
              if value > 0xffffffffffffffff, do: value = value - 0x10000000000000000
              data = Dict.put(data, key, {value, generate_expire_time(expiration), flags, exptime, work_state.current_cas})
              { :reply, {value, work_state.current_cas}, {data, WorkerState.next_cas(work_state)} }
            catch
              :error, :badarg ->
                { :reply, :invalid_incr_decr, {data, work_state} }
            end
          _ ->
            { :reply, :invalid_incr_decr, {data, work_state} }
        end
      nil when expiration == 0xffffffff ->
        { :reply, :not_found, {data, work_state} }
      _ ->
        data = Dict.put(data, key, {intial, generate_expire_time(expiration), 0, expiration, work_state.current_cas})
        { :reply, {intial, work_state.current_cas}, {data, WorkerState.next_cas(work_state)} }
    end
  end

  def handle_call({:decr, key, count, intial, expiration}, _from, {data, work_state}) do
    case Dict.get(data, key) do
      {value, timestamp, flags, exptime, _} ->
        case check_expiration(value, timestamp, exptime) do
          nil when expiration == 0xffffffff ->
            { :reply, :not_found, {HashDict.delete(data, key), work_state} }
          nil ->
            data = Dict.put(data, key, {intial, generate_expire_time(expiration), 0, expiration, work_state.current_cas})
            { :reply, {intial, work_state.current_cas}, {data, WorkerState.next_cas(work_state)} }
          value when is_integer(value) ->
            value = value - count
            if value < 0, do: value = 0
            data = Dict.put(data, key, {value, generate_expire_time(expiration), flags, exptime, work_state.current_cas})
            { :reply, {value, work_state.current_cas}, {data, WorkerState.next_cas(work_state)} }
          value when is_binary(value) ->
            try do
              value = binary_to_integer(value) - count
              if value < 0, do: value = 0
              data = Dict.put(data, key, {value, generate_expire_time(expiration), flags, exptime, work_state.current_cas})
              { :reply, {value, work_state.current_cas}, {data, WorkerState.next_cas(work_state)} }
            catch
              :error, :badarg ->
                { :reply, :invalid_incr_decr, {data, work_state} }
            end
          _ ->
            { :reply, :invalid_incr_decr, {data, work_state} }
        end
      nil when expiration == 0xffffffff ->
        { :reply, :not_found, {data, work_state} }
      _ ->
        data = Dict.put(data, key, {intial, generate_expire_time(expiration), 0, expiration, work_state.current_cas})
        { :reply, {intial, work_state.current_cas}, {data, WorkerState.next_cas(work_state)} }
    end
  end

  def handle_call({:flush, expiration}, _from, {data, work_state}) do
    expiration = cond do
      is_binary(expiration) && size(expiration) > 0 -> binary_to_integer expiration
      is_binary(expiration) -> 0
      true -> expiration
    end
    case expiration do
      0 -> { :reply, :ok, {HashDict.new, work_state} }
      _ ->
        data = Dict.keys(data)
          |> Enum.reduce data, fn(key, data) ->
            case Dict.get(data, key) do
              {value, timestamp, flags, exptime, cas} ->
                case check_expiration(value, timestamp, exptime) do
                  nil ->
                    data
                  _ ->
                    Dict.put(data, key, {value, generate_expire_time(expiration), flags, expiration, cas})
                end
              _ -> data
            end
          end
        { :reply, :ok, {data, work_state} }
    end
  end

  defp generate_expire_time(exptime) do
    cond do
      exptime < 31536000 ->
        {ms,sec,_} = :os.timestamp
        ms*1000000+sec+exptime
      true -> exptime
    end
  end

  def check_expiration data, expire_time, exptime do
    cond do
      exptime == 0 -> data
      true ->
        {ms,sec,_} = :os.timestamp
        now = ms*1000000+sec
        case now < expire_time do
          true -> data
          false -> nil
        end
    end
  end
end
