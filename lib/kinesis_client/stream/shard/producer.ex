defmodule KinesisClient.Stream.Shard.Producer do
  @moduledoc """
  Producer GenStage used in `KinesisClient.Stream.ShardConsumer` Broadway pipeline.
  """
  @behaviour Broadway.Producer

  use GenStage
  use Retry.Annotation

  alias KinesisClient.Kinesis
  alias KinesisClient.Stream.AppState
  alias KinesisClient.Stream.Coordinator

  @kinesis_records_limit 10_000

  require Logger

  defstruct [
    :kinesis_opts,
    :stream_name,
    :shard_id,
    :shard_iterator,
    :shard_iterator_type,
    :starting_sequence_number,
    :poll_interval,
    :poll_timer,
    :status,
    :notify_pid,
    :ack_ref,
    :app_name,
    :app_state_opts,
    :lease_owner,
    :shard_closed_timer,
    :coordinator_name,
    shutdown_delay: 300_000,
    demand: 0
  ]

  def start_link(opts) do
    GenStage.start_link(__MODULE__, opts)
  end

  def start(name) do
    GenServer.call(name, :start)
  end

  def stop(name) do
    GenServer.call(name, :stop)
  end

  @impl GenStage
  def init(opts) do
    state = %__MODULE__{
      shard_id: opts[:shard_id],
      app_name: opts[:app_name],
      lease_owner: opts[:lease_owner],
      kinesis_opts: opts[:kinesis_opts],
      stream_name: opts[:stream_name],
      status: opts[:status],
      app_state_opts: Keyword.get(opts, :app_state_opts, []),
      shard_iterator_type: Keyword.get(opts, :shard_iterator_type, :trim_horizon),
      poll_interval: Keyword.get(opts, :poll_interval, 5_000),
      notify_pid: Keyword.get(opts, :notify_pid),
      coordinator_name: opts[:coordinator_name]
    }

    Logger.info("Initializing KinesisClient.Stream.Shard.Producer: #{inspect(state)}")
    {:producer, state}
  end

  # Don't fetch from Kinesis if status is :stopped
  @impl GenStage
  def handle_demand(incoming_demand, %{demand: demand, status: :stopped} = state) do
    notify({:queuing_demand_while_stopped, incoming_demand}, state)

    Logger.info(
      "Shard #{state.shard_id} status is stopped, demand: #{demand}, incoming demand: #{incoming_demand}"
    )

    {:noreply, [], %{state | demand: demand + incoming_demand}}
  end

  @impl GenStage
  def handle_demand(incoming_demand, %{demand: demand, status: :closed} = state) do
    Logger.info(
      "Shard #{state.shard_id} status is closed, demand: #{demand}, incoming demand: #{incoming_demand}"
    )

    {:noreply, [], %{state | demand: demand + incoming_demand}}
  end

  @impl GenStage
  def handle_demand(incoming_demand, %{demand: demand} = state) do
    Logger.info(
      "Shard #{state.shard_id} received incoming demand: #{incoming_demand} - state demand: #{demand}"
    )

    get_records(%{state | demand: demand + incoming_demand})
  end

  @impl GenStage
  def handle_info(:get_records, %{poll_timer: nil} = state) do
    Logger.info("Poll timer is nil")
    {:noreply, [], state}
  end

  @impl GenStage
  def handle_info(:get_records, state) do
    notify(:poll_timer_executed, state)

    Logger.debug(
      "Try to fulfill pending demand #{state.demand}: " <>
        "[app_name: #{state.app_name}, shard_id: #{state.shard_id}]"
    )

    get_records(%{state | poll_timer: nil})
  end

  @impl GenStage
  def handle_info(:shard_closed, %{coordinator_name: coordinator, shard_id: shard_id} = state) do
    # just in case something goes awry, try and close the Shard in the future
    Logger.info(
      "Shard #{shard_id} is closed, notifying Coordinator: [app_name: #{state.app_name}, " <>
        "stream_name: #{state.stream_name}]"
    )

    AppState.close_shard(
      state.app_name,
      state.stream_name,
      state.shard_id,
      state.lease_owner,
      state.app_state_opts
    )

    :ok = Coordinator.close_shard(coordinator, shard_id)

    {:noreply, [], state}
  end

  @impl GenStage
  def handle_info({:ack, _ref, successful_msgs, failed_msgs}, state) do
    %{metadata: %{"SequenceNumber" => checkpoint}} = successful_msgs |> Enum.reverse() |> hd()

    :ok =
      AppState.update_checkpoint(
        state.app_name,
        state.stream_name,
        state.shard_id,
        state.lease_owner,
        checkpoint,
        state.app_state_opts
      )

    notify({:acked, %{checkpoint: checkpoint, success: successful_msgs, failed: []}}, state)

    Logger.info(
      "Acknowledged #{length(successful_msgs)} successful and #{length(successful_msgs)} failed messages: " <>
        "[app_name: #{state.app_name} shard_id: #{state.shard_id} data: #{inspect(successful_msgs)}"
    )

    {:noreply, [], state}
  end

  @impl GenStage
  def handle_info(msg, state) do
    Logger.info("ShardConsumer.Producer got an unhandled message #{inspect(msg)}")
    {:noreply, [], state}
  end

  @impl GenStage
  def handle_call(:start, from, state) do
    Logger.info("Starting KinesisClient.Stream.Shard.Producer: #{inspect(state)}")

    {:noreply, records, new_state} =
      case AppState.get_lease(
             state.app_name,
             state.stream_name,
             state.shard_id,
             state.app_state_opts
           ) do
        %{checkpoint: nil} ->
          get_records(%{
            state
            | status: :started,
              shard_iterator: nil,
              shard_iterator_type: :trim_horizon
          })

        %{checkpoint: seq_number} when is_binary(seq_number) ->
          get_records(%{
            state
            | status: :started,
              shard_iterator: nil,
              shard_iterator_type: :after_sequence_number,
              starting_sequence_number: seq_number
          })

        :not_found ->
          raise "No lease has been created for #{state.app_name}-#{state.shard_id}"
      end

    GenStage.reply(from, :ok)
    {:noreply, records, new_state}
  end

  @impl GenStage
  def handle_call(:stop, _from, state) do
    {:reply, :ok, [], %{state | status: :stopped}}
  end

  defp get_records(%__MODULE__{status: :closed} = state) do
    {:noreply, [], state}
  end

  defp get_records(%__MODULE__{shard_iterator: nil} = state) do
    state
    |> get_shard_iterator()
    |> case do
      {:ok, %{"ShardIterator" => nil}} ->
        Logger.info("Shard #{state.shard_id} has nil shard iterator")

        {:noreply, [], state}

      {:ok, %{"ShardIterator" => iterator}} ->
        get_records(%{state | shard_iterator: iterator})

      {:error, {"ResourceNotFoundException", error_message}} ->
        Logger.error(
          "Shard #{state.shard_id} (get_records).ResourceNotFoundException.#{error_message}: #{inspect(state)})"
        )

        {:noreply, [], state}
    end
  end

  defp get_records(%__MODULE__{demand: demand, kinesis_opts: kinesis_opts} = state) do
    state
    |> get_records_with_retry(Keyword.merge(kinesis_opts, limit: check_kinesis_limit(demand)))
    |> maybe_end_of_shard_reached(state)
  end

  defp check_kinesis_limit(demand) when demand <= @kinesis_records_limit, do: demand
  defp check_kinesis_limit(_demand), do: @kinesis_records_limit

  @retry with: 500 |> exponential_backoff() |> Stream.take(5)
  defp get_records_with_retry(state, kinesis_opts) do
    Kinesis.get_records(state.shard_iterator, kinesis_opts)
    |> tap(&Logger.info("Shard #{state.shard_id} Kinesis get_records_with_retry: #{inspect(&1)}"))
  end

  defp maybe_end_of_shard_reached(
         {:ok, %{"ChildShards" => _child_shards, "Records" => records}},
         state
       ) do
    new_demand = state.demand - length(records)
    state = handle_closed_shard(%{state | status: :closed, demand: new_demand})
    Logger.info("Shard #{state.shard_id} has reached the end of the shard")

    {:noreply, wrap_records(records), state}
  end

  defp maybe_end_of_shard_reached(
         {:ok, %{"NextShardIterator" => next_iterator, "Records" => records}},
         state
       ) do
    new_demand = state.demand - length(records)

    new_state =
      %{
        state
        | demand: new_demand,
          poll_timer: poll_timer({records, new_demand}, state.poll_interval),
          shard_iterator: next_iterator
      }

    {:noreply, wrap_records(records), new_state}
  end

  defp maybe_end_of_shard_reached({:error, error}, %{shard_id: shard_id} = state) do
    Logger.error("Shard #{shard_id} encountered error when getting records: #{inspect(error)}")

    {:noreply, [], state}
  end

  defp poll_timer({_, 0}, _poll_interval), do: nil
  defp poll_timer({[], _}, poll_interval), do: schedule_shard_poll(poll_interval)
  defp poll_timer(_, _poll_interval), do: schedule_shard_poll(0)

  defp get_shard_iterator(%{shard_iterator_type: :after_sequence_number} = state) do
    get_shard_iterator_with_retry(
      state.stream_name,
      state.shard_id,
      :after_sequence_number,
      Keyword.put(
        state.kinesis_opts,
        :starting_sequence_number,
        state.starting_sequence_number
      )
    )
  end

  defp get_shard_iterator(%{shard_iterator_type: :trim_horizon} = state) do
    get_shard_iterator_with_retry(
      state.stream_name,
      state.shard_id,
      :trim_horizon,
      state.kinesis_opts
    )
  end

  @retry with: 500 |> exponential_backoff() |> Stream.take(5)
  defp get_shard_iterator_with_retry(stream_name, shard_id, shard_iterator_type, kinesis_opts) do
    Kinesis.get_shard_iterator(
      stream_name,
      shard_id,
      shard_iterator_type,
      kinesis_opts
    )
  end

  # convert Kinesis records to Broadway messages
  defp wrap_records([]), do: []

  defp wrap_records(records) do
    ref = make_ref()

    Enum.map(records, fn %{"Data" => data} = record ->
      metadata = Map.delete(record, "Data")
      acknowledger = {Broadway.CallerAcknowledger, {self(), ref}, nil}
      %Broadway.Message{data: data, metadata: metadata, acknowledger: acknowledger}
    end)
  end

  defp handle_closed_shard(%{status: :closed, shard_closed_timer: nil, shutdown_delay: delay} = s) do
    timer = Process.send_after(self(), :shard_closed, delay)

    Logger.info("Handling closing shard #{inspect(s.shard_id)}")

    %{s | shard_closed_timer: timer}
  end

  defp handle_closed_shard(
         %{status: :closed, shard_closed_timer: old_timer, shutdown_delay: delay} = s
       ) do
    Process.cancel_timer(old_timer)

    timer = Process.send_after(self(), :shard_closed, delay)

    Logger.info("Handling closing shard #{inspect(s.shard_id)} again")

    %{s | shard_closed_timer: timer}
  end

  defp handle_closed_shard(state) do
    state
  end

  defp schedule_shard_poll(interval) do
    Process.send_after(self(), :get_records, interval)
  end

  defp notify(message, %__MODULE__{notify_pid: notify_pid}) do
    case notify_pid do
      pid when is_pid(pid) ->
        send(pid, message)
        :ok

      nil ->
        :ok
    end
  end
end
