defmodule KinesisClient.Stream.Rebalancer do
  @moduledoc """
  Periodically rebalances shard leases across workers.

  One Rebalancer runs per `KinesisClient.Stream` (i.e. per worker). Each tick
  it reads the per-worker lease counts once and, when this worker holds less
  than its share, picks a lease held by the most loaded worker and tells the
  local `KinesisClient.Stream.Shard.LeaseV2` process for that shard to steal
  it. The lease process stays the single writer for its shard's lease state.

  Steals are capped at `:max_leases_to_steal` per tick and the tick interval
  is jittered, so workers converge on an even distribution without several of
  them dog-piling the same lease in the same instant.
  """
  use GenServer

  import KinesisClient.Util

  alias KinesisClient.Stream.AppState
  alias KinesisClient.Stream.Shard.LeaseV2
  alias KinesisClient.Stream.Shard.LoadBalance

  require Logger

  @default_rebalance_interval 6_000
  @default_max_leases_to_steal 1

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts,
      name: register_name(__MODULE__, opts[:app_name], opts[:stream_name])
    )
  end

  defstruct [
    :app_name,
    :stream_name,
    :lease_owner,
    :app_state_opts,
    :rebalance_interval,
    :max_leases_to_steal,
    :notify
  ]

  @type t :: %__MODULE__{}

  @impl GenServer
  def init(opts) do
    state = %__MODULE__{
      app_name: opts[:app_name],
      stream_name: opts[:stream_name],
      lease_owner: opts[:lease_owner],
      app_state_opts: Keyword.get(opts, :app_state_opts, []),
      rebalance_interval: Keyword.get(opts, :rebalance_interval, @default_rebalance_interval),
      max_leases_to_steal: Keyword.get(opts, :max_leases_to_steal, @default_max_leases_to_steal),
      notify: Keyword.get(opts, :notify)
    }

    schedule_rebalance(state)

    Logger.metadata(
      kcl_app_name: state.app_name,
      kcl_stream_name: state.stream_name,
      kcl_lease_owner: state.lease_owner
    )

    Logger.info("Initializing KinesisClient.Stream.Rebalancer: #{inspect(state)}")

    {:ok, state}
  end

  @impl GenServer
  def handle_info(:rebalance, state) do
    schedule_rebalance(state)
    run_rebalance(state)
    {:noreply, state}
  rescue
    # Rebalancing is best-effort and must never take down the stream: the
    # adapters raise on transient failures (a throttled Dynamo scan, a DB
    # blip), and this process shares a :one_for_all supervisor with the
    # Coordinator and every shard pipeline. Log it and try again next tick.
    error ->
      Logger.error("Rebalancer: Rebalance tick failed: #{inspect(error)}")
      notify({:rebalance_failed, error}, state)
      {:noreply, state}
  end

  defp run_rebalance(state) do
    state.app_name
    |> AppState.total_incomplete_lease_counts_by_worker(state.stream_name, state.app_state_opts)
    |> LoadBalance.decide(state.lease_owner)
    |> case do
      :balanced ->
        notify({:all_balanced, state}, state)

      {:steal_from, victim, deficit} ->
        steal_from(victim, deficit, state)
    end
  end

  defp steal_from(victim, deficit, state) do
    state.app_name
    |> AppState.get_leases_by_worker(state.stream_name, victim, state.app_state_opts)
    |> Enum.reject(& &1.completed)
    |> Enum.shuffle()
    |> Enum.map(&local_lease_process(&1, state))
    |> Enum.reject(&is_nil/1)
    |> Enum.take(min(deficit, state.max_leases_to_steal))
    |> Enum.each(fn {shard_id, pid} ->
      Logger.debug(
        "Rebalancer: Requesting steal of shard #{shard_id} from #{victim}: " <>
          "[lease_owner: #{state.lease_owner}]"
      )

      send(pid, {:steal_lease, victim})
      notify({:steal_requested, shard_id}, state)
    end)
  end

  # The steal is executed by the shard's lease process so there is exactly one
  # writer per shard on this worker. Shards without a running local lease
  # process (not started yet, or already shut down) are skipped this tick.
  defp local_lease_process(%{shard_id: shard_id}, state) do
    state.app_name
    |> LeaseV2.whereis(state.stream_name, shard_id)
    |> case do
      nil -> nil
      pid -> {shard_id, pid}
    end
  end

  defp schedule_rebalance(%{rebalance_interval: interval}) do
    Process.send_after(self(), :rebalance, jitter(interval))
  end

  # +/- 25% so workers don't tick in lockstep and stampede the same lease.
  defp jitter(interval) do
    interval + :rand.uniform(max(div(interval, 2), 1)) - div(interval, 4)
  end

  defp notify(_msg, %{notify: nil}) do
    :ok
  end

  defp notify(msg, %{notify: notify}) do
    send(notify, msg)
    :ok
  end
end
