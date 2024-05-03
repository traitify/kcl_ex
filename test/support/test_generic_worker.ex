defmodule TestGenericWorker do
  use GenServer

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def init(initial_value) do
    {:ok, initial_value}
  end

  def get_value do
    GenServer.call(__MODULE__, :get_value)
  end

  def handle_call(:get_value, _from, state) do
    {:reply, state, state}
  end
end
