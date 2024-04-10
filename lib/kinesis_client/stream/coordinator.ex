defmodule KinesisClient.Stream.Coordinator do
  @moduledoc """
  This module will describe a given stream and enumerate all the shards. It will then handle
  starting and stopping `KinesisClient.Stream.Shard` processes as necessary to ensure the
  stream is processed completely and in the correct order.
  """
  use GenServer
  use Retry.Annotation

  alias KinesisClient.Kinesis
  alias KinesisClient.Stream.AppState
  alias KinesisClient.Stream.Shard

  require Logger

  defstruct [
    :name,
    :app_name,
    :app_state_opts,
    :stream_name,
    :shard_supervisor_name,
    :notify_pid,
    :shard_graph,
    :shard_map,
    :kinesis_opts,
    :retry_timeout,
    # unique reference used to identify this instance KinesisClient.Stream
    :worker_ref,
    :shard_args,
    shard_ref_map: %{}
  ]

  @doc """
  Starts a KinesisClient.Stream.Coordinator. KinesisClient.Stream should handle starting this.

  ## Options
    * `:name` - Required. The process name.
    * `:app_name` - Required. Will be used to name the DyanmoDB table.
    * `:stream_name` - Required. The stream to describe and start Shard workers for.
    * `:shard_supervisor_name` - Required. Needed in order to start Shard workers.
  """
  def start_link(args) do
    GenServer.start_link(__MODULE__, args, name: args[:name])
  end

  def close_shard(coordinator, shard_id) do
    GenServer.cast(coordinator, {:close_shard, shard_id})
  end

  @impl GenServer
  def init(opts) do
    state = %__MODULE__{
      name: opts[:name],
      app_name: opts[:app_name],
      app_state_opts: opts[:app_state_opts],
      stream_name: opts[:stream_name],
      shard_supervisor_name: opts[:shard_supervisor_name],
      worker_ref: opts[:worker_ref],
      shard_args: opts[:shard_args],
      notify_pid: Keyword.get(opts, :notify_pid),
      kinesis_opts: Keyword.get(opts, :kinesis_opts, []),
      retry_timeout: Keyword.get(opts, :retry_timeout, 30_000)
    }

    Logger.debug("Starting KinesisClient.Stream.Coordinates: #{inspect(state)}")
    {:ok, state, {:continue, :initialize}}
  end

  @impl GenServer
  def handle_continue(:initialize, state) do
    create_table_if_not_exists(state)
    describe_stream(state)
  end

  def handle_continue(:describe_stream, state) do
    describe_stream(state)
  end

  @impl GenServer
  def handle_call(:get_graph, _from, %{shard_graph: graph} = s) do
    {:reply, graph, s}
  end

  @impl GenServer
  def handle_cast({:close_shard, shard_id}, %{shard_ref_map: shards, stream_name: sn} = state) do
    {ref, _} = Enum.find(shards, fn {_monitor_ref, in_shard_id} -> in_shard_id == shard_id end)

    Process.demonitor(ref, [:flush])
    Shard.stop(Shard.name(sn, shard_id))

    {:noreply, state}
  end

  def create_table_if_not_exists(state) do
    opts = state.app_state_opts ++ [stream_name: state.stream_name]
    AppState.initialize(state.app_name, opts)
  end

  @spec remove_missing_parents(Map.t()) :: Map.t()
  def remove_missing_parents(shards) do
    Enum.map(shards, fn shard ->
      if Enum.any?(shards, fn %{"ShardId" => shard_id} -> shard_id == shard["ParentShardId"] end) do
        shard
      else
        Map.delete(shard, "ParentShardId")
      end
    end)
  end

  defp describe_stream(state) do
    %{"StreamStatus" => status, "Shards" => shards} =
      get_shards(state.stream_name, state.kinesis_opts)

    shards = remove_missing_parents(shards)

    notify({:shards, shards}, state)

    case status do
      "ACTIVE" ->
        shard_graph = build_shard_graph(shards)

        shard_map =
          Enum.reduce(shards, %{}, fn %{"ShardId" => shard_id} = s, acc ->
            Map.put(acc, shard_id, s)
          end)

        state = %{state | shard_map: shard_map}

        Logger.debug("(describe_stream).shard_graph: #{inspect(shard_graph)}")
        Logger.debug("(describe_stream).state: #{inspect(state)}")

        map = start_shards(shard_graph, state)

        {:noreply, %{state | shard_graph: shard_graph, shard_ref_map: map}}

      other ->
        Logger.debug("Stream is not in active state, sleeping... Stream state: #{inspect(other)}")

        :timer.sleep(state.retry_timeout)
        notify({:retrying_describe_stream, self()}, state)
        {:noreply, state, {:continue, :describe_stream}}
    end
  end

  defp build_shard_graph(shard_list) do
    graph = :digraph.new([:acyclic])

    Enum.each(shard_list, fn %{"ShardId" => shard_id} = s ->
      unless :digraph.vertex(graph, shard_id) do
        :digraph.add_vertex(graph, shard_id)
      end

      case s do
        %{"ParentShardId" => parent_shard_id} when not is_nil(parent_shard_id) ->
          add_vertex(graph, parent_shard_id)
          :digraph.add_edge(graph, parent_shard_id, shard_id, "parent-child")

          case s["AdjacentParentShardId"] do
            nil ->
              :ok

            x when is_binary(x) ->
              add_vertex(graph, x)
              :digraph.add_edge(graph, x, shard_id)
          end

        _ ->
          :ok
      end
    end)

    graph
  end

  defp start_shards(shard_graph, %__MODULE__{} = state) do
    shard_r = list_relationships(shard_graph)

    Logger.debug("(start_shards).shard_r: #{inspect(shard_r)}")

    Enum.reduce(shard_r, state.shard_ref_map, fn {shard_id, parents}, acc ->
      Logger.debug("(start_shards).shard_id: #{inspect(shard_id)}")
      Logger.debug("(start_shards).parents: #{inspect(parents)}")

      shard_lease = get_lease(shard_id, state)

      Logger.debug("(start_shards).shard_lease: #{inspect(shard_lease)}")

      case parents do
        [] ->
          case shard_lease do
            %{completed: true} ->
              acc

            _ ->
              case start_shard(shard_id, state) do
                {:ok, pid} -> Map.put(acc, Process.monitor(pid), shard_id)
              end
          end

        # handle shard splits
        [single_parent] ->
          case get_lease(single_parent, state) do
            %{completed: true} ->
              Logger.info("Parent shard #{single_parent} is completed so starting #{shard_id}")

              case start_shard(shard_id, state) do
                {:ok, pid} -> Map.put(acc, Process.monitor(pid), shard_id)
              end

            %{completed: false} ->
              Logger.info("Parent shard #{single_parent} is not completed so skipping #{shard_id}")
              acc

            :not_found ->
              Logger.info("Parent shard #{single_parent} does not exist so skipping #{shard_id}")
              acc
          end

        # handle shard merges
        [parent1, parent2] ->
          case {get_lease(parent1, state), get_lease(parent2, state)} do
            {%{completed: true}, %{completed: true}} ->
              case start_shard(shard_id, state) do
                {:ok, pid} -> Map.put(acc, Process.monitor(pid), shard_id)
              end

            _ ->
              acc
          end
      end
    end)
  end

  defp get_lease(shard_id, %{app_name: app_name, app_state_opts: app_state_opts} = state) do
    AppState.get_lease(app_name, state.stream_name, shard_id, app_state_opts)
  end

  # Start a shard and return an updated worker map with the %{lease_ref => pid}
  @spec start_shard(
          shard_id :: String.t(),
          state :: map
        ) ::
          {:ok, pid}
  defp start_shard(
         shard_id,
         %{shard_supervisor_name: shard_supervisor, shard_args: shard_args} = state
       ) do
    shard_args =
      shard_args
      |> Keyword.put(:shard_id, shard_id)
      |> Keyword.put(
        :shard_name,
        Shard.name(state.stream_name, shard_id)
      )

    {:ok, pid} = Shard.start(shard_supervisor, shard_args)
    notify({:shard_started, %{pid: pid, shard_id: shard_id}}, state)
    {:ok, pid}
  end

  ##
  # Returns a list of shape [{shard_id, [parent_shards]}]. The list is sorted from low to high on the
  # parent links list.
  defp list_relationships(graph) do
    n = graph |> :digraph.vertices() |> Enum.map(fn v -> {v, :digraph.in_neighbours(graph, v)} end)

    Enum.sort(n, fn {_, n1}, {_, n2} -> length(n1) <= length(n2) end)
  end

  defp get_shards(stream_name, kinesis_opts, shard_start_id \\ nil, shard_list \\ []) do
    {:ok, result} =
      case shard_start_id do
        nil ->
          describe_stream_with_retry(stream_name, kinesis_opts)

        x when is_binary(x) ->
          describe_stream_with_retry(
            stream_name,
            Keyword.merge(kinesis_opts, exclusive_start_shard_id: x)
          )
      end

    case result do
      %{"StreamDescription" => %{"HasMoreShards" => true, "Shards" => shards}} ->
        get_shards(
          stream_name,
          kinesis_opts,
          hd(Enum.reverse(shards))["ShardId"],
          shards ++ shard_list
        )

      %{"StreamDescription" => %{"HasMoreShards" => false, "Shards" => shards} = stream} ->
        Map.put(stream, "Shards", shards ++ shard_list)
    end
  end

  @retry with: 500 |> exponential_backoff() |> Stream.take(10)
  defp describe_stream_with_retry(stream_name, kinesis_opts) do
    Kinesis.describe_stream(stream_name, kinesis_opts)
  end

  defp add_vertex(graph, shard_id) do
    case :digraph.vertex(graph, shard_id) do
      false ->
        :digraph.add_vertex(graph, shard_id)

      _ ->
        nil
    end
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
