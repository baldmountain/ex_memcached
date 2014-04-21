defmodule MemcachedE.Worker do
  use GenServer
  require Lager

  def start_link do
      GenServer.start_link(__MODULE__, [], local: :cache)
  end

  def init([]) do
    Lager.info "Worker init"
    { :ok, {HashDict.new, 1} }
  end

  def handle_call({:get, key} , _from, {data, current_cas}) do
    case Dict.get(data, key) do
      nil ->
        # Lager.info "item not found for key #{key}"
        { :reply, :not_found, {data, current_cas} }
      {value, timestamp, flags, exptime, cas} ->
        case check_expiration(value, timestamp, exptime) do
          nil ->
            Lager.info "item expired for key #{key}"
            { :reply, :not_found, {HashDict.delete(data, key), current_cas} }
          value ->
            { :reply, {value, flags, cas}, {data, current_cas} }
        end
    end
  end

  def handle_call({:set, key, value, flags, exptime}, _from, {data, current_cas}) do
    Lager.info "set - key: #{key} len: #{size(value)}"
    data = Dict.put(data, key, {value, :os.timestamp, flags, exptime, current_cas})
    { :reply, {:stored, current_cas}, {data, current_cas + 1} }
  end

  def handle_call({:add, key, value, flags, exptime}, _from, {data, current_cas}) do
    Lager.info "add - key: #{key} len: #{size(value)}"
    case Dict.get(data, key) do
      nil ->
        data = Dict.put(data, key, {value, :os.timestamp, flags, exptime, current_cas})
        { :reply, {:stored, current_cas}, {data, current_cas + 1} }
      {_, _, _, _, _} ->
        { :reply, :exists, {data, current_cas} }
    end
  end

  def handle_call({:replace, key, value, flags, exptime}, _from, {data, current_cas}) do
    case Dict.get(data, key) do
      nil ->
        { :reply, :not_found, {data, current_cas} }
      {_, _, _, _, _} ->
        data = Dict.put(data, key, {value, :os.timestamp, flags, exptime, current_cas})
        { :reply, {:stored, current_cas}, {data, current_cas + 1} }
    end
  end

  def handle_call({:append, key, value, _flags, _exptime}, _from, {data, current_cas}) do
    case Dict.get(data, key) do
      nil ->
        { :reply, :not_stored, {data, current_cas} }
      {evalue, timestamp, eflags, eexptime, _} ->
        data = Dict.put(data, key, {evalue <> value, timestamp, eflags, eexptime, current_cas})
        { :reply, {:stored, current_cas}, {data, current_cas + 1} }
    end
  end

  def handle_call({:prepend, key, value, _flags, _exptime}, _from, {data, current_cas}) do
    case Dict.get(data, key) do
      nil ->
        { :reply, :not_stored, {data, current_cas} }
      {evalue, timestamp, eflags, eexptime, _} ->
        data = Dict.put(data, key, {value <> evalue, timestamp, eflags, eexptime, current_cas})
        { :reply, {:stored, current_cas}, {data, current_cas + 1} }
    end
  end

  def handle_call({:cas, key, value, flags, exptime, cas}, _from, {data, current_cas}) do
    case Dict.get(data, key) do
      {_, _, _, _, ^cas} ->
        data = Dict.put(data, key, {value, :os.timestamp, flags, exptime, cas})
        { :reply, {:stored, current_cas}, {data, current_cas} }
      {_, _, _, _, _} ->
        { :reply, :exists, {data, current_cas} }
      _ ->
        { :reply, :not_found, {data, current_cas} }
    end
  end

  def handle_call({:delete, key}, _from, {data, current_cas}) do
    case Dict.get(data, key) do
      {value, timestamp, _, exptime, _} ->
        case check_expiration(value, timestamp, exptime) do
          nil ->
            Lager.info "item expired for key #{key}"
            { :reply, :not_found, {HashDict.delete(data, key), current_cas} }
          value ->
            data = Dict.delete(data, key)
            { :reply, :deleted, {data, current_cas} }
        end
      _ ->
        { :reply, :not_found, {data, current_cas} }
    end
  end

  def handle_call({:incr, key, count}, _from, {data, current_cas}) do
    case Dict.get(data, key) do
      {value, timestamp, flags, exptime, _} ->
        case check_expiration(value, timestamp, exptime) do
          nil ->
            Lager.info "item expired for key #{key}"
            { :reply, :not_found, {HashDict.delete(data, key), current_cas} }
          value ->
            value = integer_to_binary(binary_to_integer(value) + count)
            data = Dict.put(data, key, {value, :os.timestamp, flags, exptime, current_cas})
            { :reply, value, {data, current_cas + 1} }
        end
      _ ->
        { :reply, :not_found, {data, current_cas} }
    end
  end

  def handle_call({:decr, key, count}, _from, {data, current_cas}) do
    case Dict.get(data, key) do
      {value, timestamp, flags, exptime, _} ->
        case check_expiration(value, timestamp, exptime) do
          nil ->
            Lager.info "item expired for key #{key}"
            { :reply, :not_found, {HashDict.delete(data, key), current_cas} }
          value ->
            value = binary_to_integer(value) - count
            if value < 0, do: value = 0
            value = integer_to_binary(value)
            data = Dict.put(data, key, {value, :os.timestamp, flags, exptime, current_cas})
            { :reply, value, {data, current_cas + 1} }
        end
      _ ->
        { :reply, :not_found, {data, current_cas} }
    end
  end

  def check_expiration data, timestamp, exptime do
    cond do
      exptime == 0 -> data
      exptime < 31536000 ->
        {ems,esec,_} = timestamp
        {ms,sec,_} = :os.timestamp
        cond do
          ems*1000000+esec+exptime < ms*1000000+sec -> data
          true -> nil
        end
      true ->
        {ms,sec,_} = :os.timestamp
        cond do
          exptime < ms*1000000+sec -> data
          true -> nil
        end
    end
  end
end

# - "STORED\r\n", to indicate success.
#
# - "NOT_STORED\r\n" to indicate the data was not stored, but not
# because of an error. This normally means that the
# condition for an "add" or a "replace" command wasn't met.
#
# - "EXISTS\r\n" to indicate that the item you are trying to store with
# a "cas" command has been modified since you last fetched it.
#
# - "NOT_FOUND\r\n" to indicate that the item you are trying to store
# with a "cas" command did not exist.
