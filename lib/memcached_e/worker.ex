defmodule MemcachedE.Worker do
  use GenServer
  require Lager

  @cache_timout 30*1000*1000

  def start_link do
      GenServer.start_link(__MODULE__, [], local: :cache)
  end

  def init([]) do
    Lager.info "Worker init}"
    { :ok, { HashDict.new, @cache_timout } }
  end

  def handle_call({:get, key} , _from, {data, cache_timeout}) do
    case Dict.get data, key do
      {value, timestamp} ->
        case :timer.now_diff(:os.timestamp, timestamp) > cache_timeout do
          true ->
            Lager.info "item expired for key #{key}"
            data = Dict.delete data, key
            { :reply, nil, {data, cache_timeout} }
          _ ->
            Lager.info "item is #{:timer.now_diff(:os.timestamp, timestamp)/1000000} seconds old"
            { :reply, value, {data, cache_timeout} }
        end
      nil ->
        Lager.info "item not found for key #{key}"
        { :reply, nil, {data, cache_timeout} }
    end

  end

  def handle_cast({:put, key, value }, {data, cache_timeout}) do
    data = Dict.put(data, key, {value, :os.timestamp})
    { :noreply, {data, cache_timeout} }
  end
end
