defmodule KinesisClient.Stream.Shard.LeaseV2 do
  @moduledoc """
  Lease management for a single Kinesis shard.

  Each shard has a corresponding "lease" entry in the shard_lease table. This
  process creates the lease if missing, renews it while held, and takes over
  expired leases when their owner stops renewing (crash detection). It also
  executes lease steals on behalf of `KinesisClient.Stream.Rebalancer`, which
  makes the load balancing decisions once per worker and sends a
  `:steal_lease` message to the shard it wants: this process stays the single
  writer for its shard's lease state.
  """
  use GenServer

  import KinesisClient.Util

  alias KinesisClient.Stream.AppState
  alias KinesisClient.Stream.Shard.Pipeline

  require Logger

  @default_renew_interval 30_000
  @default_lease_expiry 45_001

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts,
      name: register_name(__MODULE__, opts[:app_name], opts[:stream_name], [opts[:shard_id]])
    )
  end

  @doc """
  Returns the pid of the locally registered lease process for `shard_id`, or
  `nil` if none is running.
  """
  @spec whereis(String.t(), String.t(), String.t()) :: pid() | nil
  def whereis(app_name, stream_name, shard_id) do
    __MODULE__
    |> register_name(app_name, stream_name, [shard_id])
    |> Process.whereis()
  end

  defstruct [
    :app_name,
    :stream_name,
    :shard_id,
    :lease_owner,
    :lease_count,
    :lease_count_increment_time,
    :renew_interval,
    :app_state_opts,
    :notify,
    :lease_expiry,
    :lease_holder,
    :pipeline
  ]

  @type t :: %__MODULE__{}

  @impl GenServer
  def init(opts) do
    state = %__MODULE__{
      app_name: opts[:app_name],
      stream_name: opts[:stream_name],
      shard_id: opts[:shard_id],
      lease_owner: opts[:lease_owner],
      app_state_opts: Keyword.get(opts, :app_state_opts, []),
      renew_interval: Keyword.get(opts, :renew_interval, @default_renew_interval),
      lease_expiry: Keyword.get(opts, :lease_expiry, @default_lease_expiry),
      lease_holder: Keyword.get(opts, :lease_holder, false),
      lease_count_increment_time: current_time(),
      notify: Keyword.get(opts, :notify),
      pipeline: Keyword.get(opts, :pipeline, Pipeline)
    }

    Process.send_after(self(), :take_or_renew_lease, state.renew_interval)

    Logger.metadata(
      kcl_app_name: state.app_name,
      kcl_stream_name: state.stream_name,
      kcl_shard_id: state.shard_id,
      kcl_lease_owner: state.lease_owner
    )

    Logger.info("Initializing KinesisClient.Stream.LeaseV2 with load balancing: #{inspect(state)}")

    {:ok, state, {:continue, :initialize}}
  end

  @impl GenServer
  def handle_continue(:initialize, state) do
    new_state =
      state
      |> get_shard_lease()
      |> case do
        :not_found ->
          Logger.debug(
            "ShardLease: No existing lease record found in AppState: [shard_id: #{state.shard_id}]"
          )

          create_shard_lease(state)

        shard_lease ->
          Logger.debug(
            "ShardLease: Found existing lease record in AppState: " <>
              "[shard_id: #{state.shard_id}, lease_owner: #{shard_lease.lease_owner}]"
          )

          set_lease_count(shard_lease.lease_count, false, state)
      end

    if new_state.lease_holder do
      :ok = state.pipeline.start(state)
    end

    notify({:initialized, new_state}, state)

    {:noreply, new_state}
  end

  @impl GenServer
  def handle_info(:take_or_renew_lease, state) do
    Process.send_after(self(), :take_or_renew_lease, state.renew_interval)

    state
    |> get_shard_lease()
    |> case do
      {:error, e} ->
        Logger.error("ShardLease: Error fetching lease for shard #{state.shard_id}: #{inspect(e)}")
        {:noreply, state}

      :not_found ->
        Logger.error("ShardLease: Unable to find lease for shard #{state.shard_id}")
        {:noreply, state}

      shard_lease ->
        Logger.debug(
          "ShardLease: Running take_or_renew_lease process for shard #{state.shard_id}, lease_owner: #{state.lease_owner}, current_owner: #{shard_lease.lease_owner}"
        )

        {:noreply, take_or_renew_lease(shard_lease, state)}
    end
  end

  # Sent by KinesisClient.Stream.Rebalancer when it decides this worker should
  # steal this shard's lease from `victim`, the overloaded worker.
  @impl GenServer
  def handle_info({:steal_lease, _victim}, %{lease_holder: true} = state) do
    {:noreply, state}
  end

  def handle_info({:steal_lease, victim}, state) do
    state
    |> get_shard_lease()
    |> case do
      {:error, e} ->
        Logger.error("ShardLease: Error fetching lease for shard #{state.shard_id}: #{inspect(e)}")
        {:noreply, state}

      :not_found ->
        Logger.error("ShardLease: Unable to find lease for shard #{state.shard_id}")
        {:noreply, state}

      shard_lease ->
        {:noreply, maybe_steal(shard_lease, victim, state)}
    end
  end

  defp get_shard_lease(state) do
    AppState.get_lease(state.app_name, state.stream_name, state.shard_id, state.app_state_opts)
  end

  defp create_shard_lease(
         %{app_state_opts: opts, app_name: app_name, lease_owner: lease_owner} = state
       ) do
    Logger.debug(
      "ShardLease: Creating lease [app_name: #{app_name}, shard_id: #{state.shard_id}, " <>
        "lease_owner: #{lease_owner}]"
    )

    app_name
    |> AppState.create_lease(state.stream_name, state.shard_id, lease_owner, opts)
    |> case do
      :ok -> set_lease_count(1, true, state)
      :already_exists -> %{state | lease_holder: false}
    end
  end

  # The AppState row names this worker as owner while lease_holder is false —
  # e.g. a renewal that "failed" after actually being applied (lost response
  # + AWS retry fails the conditional check), or this process restarted after
  # taking the lease. Neither renewing (requires lease_holder) nor taking
  # (both adapters reject taking a lease you already own) can recover from
  # here, so without this clause the shard would sit unconsumed forever.
  # Renew under the optimistic lock and resume the pipeline; if another
  # worker took the lease in the meantime, the renewal fails and we keep
  # tracking.
  defp reclaim_shard_lease(shard_lease, %{app_state_opts: opts, app_name: app_name} = state) do
    expected = shard_lease.lease_count + 1

    case AppState.renew_lease(app_name, state.stream_name, shard_lease, opts) do
      {:ok, ^expected} ->
        Logger.info(
          "ShardLease: Reclaimed own lease that was not being held: " <>
            "[shard_id: #{state.shard_id}, lease_owner: #{state.lease_owner}]"
        )

        expected
        |> set_lease_count(true, state)
        |> tap(&notify({:lease_reclaimed, &1}, &1))
        |> tap(fn state -> state.pipeline.start(state) end)

      {:error, error} ->
        Logger.error(
          "ShardLease: Failed to reclaim own lease, error: #{inspect(error)}, " <>
            "[shard_id: #{state.shard_id}, lease_owner: #{state.lease_owner}]"
        )

        state
    end
  end

  defp renew_shard_lease(shard_lease, %{app_state_opts: opts, app_name: app_name} = state) do
    expected = shard_lease.lease_count + 1

    case AppState.renew_lease(app_name, state.stream_name, shard_lease, opts) do
      {:ok, ^expected} ->
        Logger.debug(
          "ShardLease: Renewing lease: [app_name: #{app_name}, shard_id: #{state.shard_id}, lease_owner: #{state.lease_owner}]"
        )

        expected
        |> set_lease_count(true, state)
        |> tap(&notify({:lease_renewed, &1}, &1))

      {:error, :lease_renew_failed} ->
        Logger.error(
          "ShardLease: Failed to renew lease, stopping pipeline: [app_name: #{app_name}, " <>
            "shard_id: #{state.shard_id}, lease_owner: #{state.lease_owner}, current_owner: #{shard_lease.lease_owner}]"
        )

        :ok = state.pipeline.stop(state)

        %{state | lease_holder: false, lease_count_increment_time: current_time()}
        |> tap(&notify({:lease_renew_failed, &1}, &1))

      {:error, error} ->
        Logger.error(
          "ShardLease: Error trying to renew lease, error: #{inspect(error)}, [app_name: #{app_name}, " <>
            "shard_id: #{state.shard_id}, lease_owner: #{state.lease_owner}], current_owner: #{shard_lease.lease_owner}"
        )

        state
    end
  end

  defp take_shard_lease(shard_lease, %{app_state_opts: opts, app_name: app_name} = state) do
    expected = state.lease_count + 1

    case AppState.take_lease(
           app_name,
           state.stream_name,
           state.shard_id,
           state.lease_owner,
           state.lease_count,
           opts
         ) do
      {:ok, ^expected} ->
        Logger.debug(
          "ShardLease: Taking lease: [shard_id: #{state.shard_id}, lease_owner: #{state.lease_owner}]"
        )

        expected
        |> set_lease_count(true, state)
        |> tap(fn state -> notify({:lease_taken, state}, state) end)
        |> tap(fn state -> state.pipeline.start(state) end)

      {:error, error} ->
        Logger.error(
          "ShardLease: Error trying to take lease for shard #{state.shard_id}, lease_owner: #{state.lease_owner}, " <>
            "current_owner: #{shard_lease.lease_owner}, error: #{inspect(error)}"
        )

        %{state | lease_holder: false, lease_count_increment_time: current_time()}
    end
  end

  defp maybe_steal(%{completed: true}, _victim, state), do: state

  defp maybe_steal(%{lease_owner: victim} = shard_lease, victim, %{lease_owner: me} = state)
       when victim != me do
    steal_shard_lease(shard_lease, state)
  end

  # The lease changed hands between the rebalancer's decision and this
  # message (or is already ours) — stealing now would hit the wrong worker,
  # possibly one that is not overloaded at all. Let the next tick re-decide.
  defp maybe_steal(shard_lease, victim, state) do
    Logger.debug(
      "ShardLease: Skipping steal, lease is no longer owned by the chosen victim: " <>
        "[shard_id: #{state.shard_id}, lease_owner: #{state.lease_owner}, " <>
        "victim: #{victim}, current_owner: #{shard_lease.lease_owner}]"
    )

    state
  end

  defp steal_shard_lease(shard_lease, state) do
    # Use the lease_count from the freshly read shard_lease rather than the copy
    # in state: state.lease_count is only synced on the (slower) renew tick, and
    # a stale count would fail the optimistic-lock check on every steal attempt.
    state.app_name
    |> AppState.take_lease(
      state.stream_name,
      state.shard_id,
      state.lease_owner,
      shard_lease.lease_count,
      state.app_state_opts
    )
    |> case do
      {:ok, new_lease_count} ->
        Logger.debug("ShardLease: Successfully stole lease for #{state.shard_id}")

        new_lease_count
        |> set_lease_count(true, state)
        |> tap(fn state -> notify({:lease_stolen, state}, state) end)
        |> tap(fn state -> state.pipeline.start(state) end)

      {:error, error} ->
        Logger.error(
          "ShardLease: Error trying to steal lease for #{state.shard_id}, lease_owner: #{state.lease_owner}, " <>
            "current_owner: #{shard_lease.lease_owner}, error: #{inspect(error)}"
        )

        state
    end
  end

  defp take_or_renew_lease(
         shard_lease,
         %{lease_expiry: lease_expiry, lease_count_increment_time: lcit} = state
       ) do
    cond do
      shard_lease.lease_owner == state.lease_owner and state.lease_holder ->
        Logger.debug(
          "ShardLease: Renewing lease: [shard_id: #{state.shard_id}, lease_owner: #{state.lease_owner}, lease_holder: #{state.lease_holder}]"
        )

        renew_shard_lease(shard_lease, state)

      shard_lease.lease_owner == state.lease_owner ->
        reclaim_shard_lease(shard_lease, state)

      shard_lease.lease_owner != state.lease_owner and state.lease_holder ->
        Logger.debug(
          "ShardLease: Lease lost to another worker, stopping pipeline: [shard_id: #{state.shard_id}, " <>
            "lease_owner: #{state.lease_owner}, current_owner: #{shard_lease.lease_owner}]"
        )

        :ok = state.pipeline.stop(state)

        set_lease_count(shard_lease.lease_count, false, state)

      current_time() - lcit > lease_expiry ->
        Logger.debug(
          "ShardLease: Lease expired, attempting to take lease: [shard_id: #{state.shard_id}, lease_holder: #{state.lease_holder}, " <>
            "lease_count_increment_time: #{lcit}}, lease_owner: #{state.lease_owner}, lease_count: #{state.lease_count}, " <>
            "current_owner: #{shard_lease.lease_owner}, current_lease_count: #{shard_lease.lease_count}]"
        )

        take_shard_lease(shard_lease, state)

      true ->
        if shard_lease.lease_count != state.lease_count do
          Logger.debug(
            "ShardLease: Set lease count to match AppState record: [shard_id: #{state.shard_id}, " <>
              "lease_owner: #{state.lease_owner}, lease_count: #{shard_lease.lease_count}]"
          )

          set_lease_count(shard_lease.lease_count, false, state)
        else
          state
        end
        |> tap(fn state ->
          Logger.debug(
            "ShardLease: Lease is owned by another node [shard_id: #{state.shard_id}, lease_holder: #{state.lease_holder}, " <>
              "lease_owner: #{state.lease_owner}, lease_count: #{state.lease_count}, " <>
              "current_owner: #{shard_lease.lease_owner}, current_lease_count: #{shard_lease.lease_count}]"
          )
        end)
        |> tap(&notify({:tracking_lease, &1}, &1))
    end
  end

  defp set_lease_count(lease_count, is_lease_holder, %__MODULE__{} = state) do
    %{
      state
      | lease_count: lease_count,
        lease_holder: is_lease_holder,
        lease_count_increment_time: current_time()
    }
  end

  defp notify(_msg, %{notify: nil}) do
    :ok
  end

  defp notify(msg, %{notify: notify}) do
    send(notify, msg)
    :ok
  end

  defp current_time() do
    System.monotonic_time(:millisecond)
  end
end
