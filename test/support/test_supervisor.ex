defmodule TestSupervisor do
  use Supervisor

  def start_link(opts) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def init(_opts) do
    children = [
      %{
        id: TestGenericWorker,
        start: {TestGenericWorker, :start_link, [arg_value: "initial_value"]}, # pass initial configuration if necessary
        type: :worker,
        restart: :permanent
      }
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
