defmodule KinesisClient.Stream.Shard.LeaseV2 do
  @moduledoc """
  Load-balanced lease management for Kinesis shards.

  This module implements a new load balancing mechanism where:
  1. Each shard has a corresponding "lease" entry in the shard_lease table
  2. Workers can steal leases from overloaded workers
  3. Load balancing algorithm distributes shards evenly across workers
  4. Periodic rebalancing and crash detection ensure optimal distribution
  """
  use GenServer

  import KinesisClient.Util

  alias KinesisClient.Stream.AppState
  alias KinesisClient.Stream.Shard.LoadBalance
  alias KinesisClient.Stream.Shard.Pipeline

  require Logger

  @default_renew_interval 30_000
  @default_lease_expiry 45_001
  @default_rebalance_interval 6_000

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts,
      name: register_name(__MODULE__, opts[:app_name], opts[:stream_name], [opts[:shard_id]])
    )
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
    :pipeline,
    :rebalance_interval
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
      pipeline: Keyword.get(opts, :pipeline, Pipeline),
      rebalance_interval: Keyword.get(opts, :rebalance_interval, @default_rebalance_interval)
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
          Logger.info(
            "ShardLease: No existing lease record found in AppState: [shard_id: #{state.shard_id}]"
          )

          create_shard_lease(state)

        shard_lease ->
          Logger.info(
            "ShardLease: Found existing lease record in AppState, attempting load balancing: " <>
              "[shard_id: #{state.shard_id}, lease_owner: #{shard_lease.lease_owner}]"
          )

          shard_lease.lease_count
          |> set_lease_count(false, state)
          |> then(&load_balancing(&1, state))
      end

    if new_state.lease_holder do
      :ok = state.pipeline.start(state)
    end

    Process.send_after(self(), :rebalance, new_state.rebalance_interval)

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
        Logger.info(
          "ShardLease: Running take_or_renew_lease process for shard #{state.shard_id}, lease_owner: #{state.lease_owner}, current_owner: #{shard_lease.lease_owner}"
        )

        {:noreply, take_or_renew_lease(shard_lease, state)}
    end
  end

  @impl GenServer
  def handle_info(:rebalance, state) do
    Process.send_after(self(), :rebalance, state.rebalance_interval)

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
        Logger.info(
          "ShardLease: Running rebalance process for shard #{state.shard_id}, lease_owner: #{state.lease_owner}, current_owner: #{shard_lease.lease_owner}"
        )

        {:noreply, load_balancing(shard_lease, state)}
    end
  end

  defp get_shard_lease(state) do
    AppState.get_lease(state.app_name, state.stream_name, state.shard_id, state.app_state_opts)
  end

  defp create_shard_lease(
         %{app_state_opts: opts, app_name: app_name, lease_owner: lease_owner} = state
       ) do
    Logger.info(
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

  defp renew_shard_lease(shard_lease, %{app_state_opts: opts, app_name: app_name} = state) do
    expected = shard_lease.lease_count + 1

    case AppState.renew_lease(app_name, state.stream_name, shard_lease, opts) do
      {:ok, ^expected} ->
        Logger.info(
          "ShardLease: Renewing lease: [app_name: #{app_name}, shard_id: #{state.shard_id}, lease_owner: #{state.lease_owner}]"
        )

        expected
        |> set_lease_count(true, state)
        |> tap(&notify({:lease_renewed, &1}, &1))

      {:error, error} ->
        Logger.error(
          "ShardLease: Failed to renew lease, error: #{inspect(error)}, [app_name: #{app_name}, " <>
            "shard_id: #{state.shard_id}, lease_owner: #{state.lease_owner}]"
        )

        state
    end
  end

  defp take_shard_lease(%{app_state_opts: opts, app_name: app_name} = state) do
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
        Logger.info(
          "ShardLease: Taking lease: [shard_id: #{state.shard_id}, lease_owner: #{state.lease_owner}]"
        )

        expected
        |> set_lease_count(true, state)
        |> tap(fn state -> notify({:lease_taken, state}, state) end)
        |> tap(fn state -> Pipeline.start(state) end)

      {:error, error} ->
        Logger.error(
          "ShardLease: Error trying to take lease for shard #{state.shard_id}, error: #{inspect(error)}"
        )

        %{state | lease_holder: false, lease_count_increment_time: current_time()}
    end
  end

  defp steal_shard_lease(state) do
    state.app_name
    |> AppState.take_lease(
      state.stream_name,
      state.shard_id,
      state.lease_owner,
      state.lease_count,
      state.app_state_opts
    )
    |> case do
      {:ok, new_lease_count} ->
        Logger.info("ShardLease: Successfully stole lease for #{state.shard_id}")

        new_lease_count
        |> set_lease_count(true, state)
        |> tap(fn state -> notify({:lease_stolen, state}, state) end)
        |> tap(fn state -> Pipeline.start(state) end)

      {:error, error} ->
        Logger.error(
          "ShardLease: Error trying to steal lease for #{state.shard_id}, error: #{inspect(error)}"
        )

        state
    end
  end

  defp load_balancing(shard_lease, state) do
    state
    |> LoadBalance.check_if_balanced?()
    |> case do
      true ->
        Logger.info(
          "ShardLease: Current load is balanced - no action needed: [shard_id: #{state.shard_id}, lease_owner: #{state.lease_owner}]"
        )

        state

      false ->
        Logger.info(
          "ShardLease: Load is unbalanced, evaluating lease stealing options: [shard_id: #{state.shard_id}, lease_owner: #{state.lease_owner}]"
        )

        attempt_to_steal(shard_lease, state)
    end
  end

  defp take_or_renew_lease(
         shard_lease,
         %{lease_expiry: lease_expiry, lease_count_increment_time: lcit} = state
       ) do
    cond do
      shard_lease.lease_owner == state.lease_owner and state.lease_holder ->
        Logger.info(
          "ShardLease: Renewing lease: [shard_id: #{state.shard_id}, lease_owner: #{state.lease_owner}, lease_holder: #{state.lease_holder}]"
        )

        renew_shard_lease(shard_lease, state)

      shard_lease.lease_owner != state.lease_owner and state.lease_holder ->
        Logger.info(
          "ShardLease: Lease lost to another worker, stopping pipeline: [shard_id: #{state.shard_id}, " <>
            "lease_owner: #{state.lease_owner}, current_owner: #{shard_lease.lease_owner}]"
        )

        :ok = state.pipeline.stop(state)

        set_lease_count(shard_lease.lease_count, false, state)

      current_time() - lcit > lease_expiry ->
        Logger.info(
          "ShardLease: Lease expired, attempting to take lease: [shard_id: #{state.shard_id}, lease_owner: #{state.lease_owner}, " <>
            "current_owner: #{shard_lease.lease_owner}, lease_count: #{state.lease_count}, current_lease_count: #{shard_lease.lease_count}]"
        )

        take_shard_lease(state)

      true ->
        if shard_lease.lease_count != state.lease_count do
          Logger.info(
            "ShardLease: Set lease count to match AppState record: [shard_id: #{state.shard_id}, " <>
              "lease_owner: #{state.lease_owner}, lease_count: #{shard_lease.lease_count}]"
          )

          set_lease_count(shard_lease.lease_count, false, state)
        else
          state
        end
        |> tap(fn state ->
          Logger.info(
            "ShardLeaase: Lease is owned by another node [shard_id: #{state.shard_id}, " <>
              "lease_owner: #{state.lease_owner}, lease_count: #{state.lease_count}]"
          )
        end)
        |> tap(&notify({:tracking_lease, &1}, &1))
    end
  end

  defp attempt_to_steal(shard_lease, state) do
    case shard_lease.lease_owner != state.lease_owner do
      true ->
        Logger.info(
          "ShardLease: Current worker does not own the lease, attempting to steal from overloaded worker: [shard_id: #{state.shard_id}, " <>
            "lease_owner: #{state.lease_owner}, current_owner: #{shard_lease.lease_owner}]"
        )

        steal_lease_from_overloaded_worker(state)

      false ->
        Logger.info(
          "ShardLease: Current worker already owns the lease, no action needed: [shard_id: #{state.shard_id}, " <>
            "lease_owner: #{state.lease_owner}, current_owner: #{shard_lease.lease_owner}]"
        )

        state
    end
  end

  defp steal_lease_from_overloaded_worker(state) do
    case LoadBalance.find_worker_with_most_leases(state) do
      [] ->
        Logger.debug(
          "ShardLease: No overloaded workers found to steal from for shard #{state.shard_id}"
        )

        state

      shard_leases ->
        shard_leases
        |> Enum.find(fn shard_lease -> shard_lease.shard_id == state.shard_id end)
        |> case do
          nil ->
            Logger.info(
              "ShardLease: Shard #{state.shard_id} does not belong to an overloaded worker"
            )

            state

          shard_lease ->
            Logger.info(
              "ShardLease: Attempting to steal lease for shard #{state.shard_id} [lease_owner: #{state.lease_owner}, " <>
                "current_owner: #{shard_lease.lease_owner}, lease_count: #{state.lease_count}, current_lease_count: #{shard_lease.lease_count}]"
            )

            steal_shard_lease(state)
        end
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
