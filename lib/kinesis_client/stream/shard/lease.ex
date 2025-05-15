defmodule KinesisClient.Stream.Shard.Lease do
  @moduledoc false
  use GenServer

  import KinesisClient.Util

  alias KinesisClient.Stream.AppState
  alias KinesisClient.Stream.Shard.Pipeline

  require Logger

  @default_renew_interval 30_000
  # The amount of time that must have elapsed since the least_count was incremented in order to
  # consider the lease expired.
  @default_lease_expiry 45_001
  @default_renewal_limit 10
  @no_limit -1

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
    :app_state_opts,
    :renew_interval,
    :notify,
    :lease_expiry,
    :lease_holder,
    :lease_renewal_limit,
    :spread_lease,
    :lease_renewal_count,
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
      lease_renewal_limit: Keyword.get(opts, :lease_renewal_limit, @default_renewal_limit),
      spread_lease: Keyword.get(opts, :spread_lease, false),
      lease_renewal_count: Keyword.get(opts, :lease_renewal_count, 0),
      lease_holder: Keyword.get(opts, :lease_holder, false),
      lease_count_increment_time: current_time(),
      notify: Keyword.get(opts, :notify),
      pipeline: Keyword.get(opts, :pipeline, Pipeline)
    }

    Logger.metadata(
      kcl_app_name: state.app_name,
      kcl_stream_name: state.stream_name,
      kcl_shard_id: state.shard_id,
      kcl_lease_owner: state.lease_owner
    )

    Logger.info("Initializing KinesisClient.Stream.Lease: #{inspect(state)}")

    {:ok, state, {:continue, :initialize}}
  end

  @impl GenServer
  def handle_continue(:initialize, state) do
    new_state =
      case get_lease(state) do
        :not_found ->
          Logger.debug(
            "No existing lease record found in AppState: " <>
              "[app_name: #{state.app_name}, shard_id: #{state.shard_id}]"
          )

          if total_number_of_leases(state) == 1 do
            Process.send_after(self(), :take_or_renew_lease, state.renew_interval)

            create_lease(state)
          else
            Process.send_after(self(), :create_lease, state.renew_interval)

            state
          end

        s ->
          Process.send_after(self(), :take_or_renew_lease, state.renew_interval)

          take_or_renew_lease(s, state)
      end

    if new_state.lease_holder do
      :ok = state.pipeline.start(state)
    end

    notify({:initialized, new_state}, state)

    {:noreply, new_state}
  end

  @impl GenServer
  def handle_info(:create_lease, state) do
    Process.send_after(self(), :take_or_renew_lease, state.renew_interval)

    new_state = create_lease(state)

    if new_state.lease_holder do
      :ok = state.pipeline.start(state)
    end

    notify({:lease_created, new_state}, state)

    {:noreply, new_state}
  end

  @impl GenServer
  def handle_info(:take_or_renew_lease, state) do
    Process.send_after(self(), :take_or_renew_lease, state.renew_interval)

    case get_lease(state) do
      {:error, e} ->
        Logger.error("Error fetching shard #{state.shard_id}: #{inspect(e)}")
        {:noreply, state}

      :not_found ->
        Logger.error("shard #{state.shard_id} not found")
        {:noreply, state}

      s ->
        {:noreply, take_or_renew_lease(s, state)}
    end
  end

  defp take_or_renew_lease(
         shard_lease,
         %{lease_expiry: lease_expiry, lease_count_increment_time: lcit} = state
       ) do
    cond do
      shard_lease.lease_owner == state.lease_owner and state.lease_holder ->
        renew_lease(shard_lease, state)

      current_time() - lcit > lease_expiry * total_number_of_leases(state) ->
        take_lease(shard_lease, state)

      true ->
        state =
          if shard_lease.lease_count != state.lease_count do
            set_lease_count(shard_lease.lease_count, false, state)
          else
            %{state | lease_holder: false}
          end

        Logger.info(
          "Lease is owned by another node, and could not be taken: [shard_id: #{state.shard_id}, " <>
            "lease_owner: #{state.lease_owner}, lease_count: #{state.lease_count}]"
        )

        notify({:tracking_lease, state}, state)
        state
    end
  end

  defp set_lease_count(lease_count, is_lease_holder, %__MODULE__{} = state) do
    %{
      state
      | lease_count: lease_count,
        lease_count_increment_time: current_time(),
        lease_holder: is_lease_holder
    }
  end

  defp get_lease(state) do
    AppState.get_lease(state.app_name, state.stream_name, state.shard_id, state.app_state_opts)
  end

  defp create_lease(%{app_state_opts: opts, app_name: app_name, lease_owner: lease_owner} = state) do
    Logger.debug(
      "Creating lease: [app_name: #{app_name}, shard_id: #{state.shard_id}, lease_owner: " <>
        "#{lease_owner}]"
    )

    case AppState.create_lease(app_name, state.stream_name, state.shard_id, lease_owner, opts) do
      :ok -> %{state | lease_holder: true, lease_count: 1}
      :already_exists -> %{state | lease_holder: false}
    end
  end

  defp renew_lease(_shard_lease, %{lease_renewal_limit: limit, lease_renewal_count: count} = state)
       when count == limit do
    Logger.info("Releasing lease: shard_id: #{state.shard_id}, worker: #{state.lease_owner}")

    %{
      state
      | lease_holder: false,
        lease_count_increment_time: current_time(),
        lease_renewal_count: 0
    }
    |> tap(&notify({:lease_released, &1}, &1))
    |> tap(&Pipeline.stop(&1))
  end

  defp renew_lease(shard_lease, %{app_state_opts: opts, app_name: app_name} = state) do
    expected = shard_lease.lease_count + 1

    Logger.info(
      "Renewing lease: [app_name: #{app_name}, shard_id: #{state.shard_id}, lease_owner: " <>
        "#{state.lease_owner}]"
    )

    case AppState.renew_lease(app_name, state.stream_name, shard_lease, opts) do
      {:ok, ^expected} ->
        expected
        |> set_lease_count(true, state)
        |> then(fn state -> %{state | lease_renewal_count: state.lease_renewal_count + 1} end)
        |> tap(&notify({:lease_renewed, &1}, &1))

      {:error, :lease_renew_failed} ->
        Logger.error(
          "Failed to renew lease, stopping producer: [app_name: #{app_name}, " <>
            "shard_id: #{state.shard_id}, lease_owner: #{state.lease_owner}]"
        )

        :ok = Pipeline.stop(state)

        %{
          state
          | lease_holder: false,
            lease_count_increment_time: current_time(),
            lease_renewal_count: 0
        }

      {:error, e} ->
        Logger.error("Error trying to renew lease for #{state.shard_id}: #{inspect(e)}")
        state
    end
  end

  defp take_lease(_shard_lease, %{app_state_opts: opts, app_name: app_name} = state) do
    expected = state.lease_count + 1

    Logger.info(
      "Attempting to take lease: [lease_owner: #{state.lease_owner}, shard_id: #{state.shard_id}]"
    )

    case AppState.take_lease(
           app_name,
           state.stream_name,
           state.shard_id,
           state.lease_owner,
           state.lease_count,
           opts
         ) do
      {:ok, ^expected} ->
        state =
          %{
            state
            | lease_holder: true,
              lease_count: expected,
              lease_count_increment_time: current_time()
          }
          |> set_lease_renewal_limit()

        notify({:lease_taken, state}, state)
        :ok = Pipeline.start(state)
        state

      {:error, :lease_take_failed} ->
        # TODO
        # :ok = Processor.ensure_halted(state)
        %{state | lease_holder: false, lease_count_increment_time: current_time()}

      {:error, e} ->
        Logger.error("Error trying to take lease for #{state.shard_id}: #{inspect(e)}")
        state
    end
  end

  defp notify(_msg, %{notify: nil}) do
    :ok
  end

  defp notify(msg, %{notify: notify}) do
    send(notify, msg)
    :ok
  end

  defp current_time do
    System.monotonic_time(:millisecond)
  end

  defp total_number_of_leases(%{spread_lease: false}), do: 1

  defp total_number_of_leases(state) do
    AppState.get_leases_by_worker(
      state.app_name,
      state.stream_name,
      state.lease_owner,
      state.app_state_opts
    )
    |> case do
      [] -> 1
      leases -> length(leases) + 1
    end
  end

  defp set_lease_renewal_limit(state) do
    AppState.get_leases_by_worker(
      state.app_name,
      state.stream_name,
      state.lease_owner,
      state.app_state_opts
    )
    |> length()
    |> case do
      1 -> %{state | lease_renewal_limit: @no_limit}
      _ -> state
    end
  end
end
