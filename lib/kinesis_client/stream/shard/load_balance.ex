defmodule KinesisClient.Stream.Shard.LoadBalance do
  @moduledoc """
  This module will implement the load balancing logic for Kinesis shard leases.
  It will provide functions to distribute shard leases evenly across workers,
  detect overloaded workers, and facilitate lease stealing.
  """

  alias KinesisClient.Stream.AppState
  alias KinesisClient.Stream.AppState.Ecto.ShardLease
  alias KinesisClient.Stream.AppState.Ecto.ShardLeases

  @spec find_worker_with_most_leases(map()) :: list(ShardLease.t())
  def find_worker_with_most_leases(state) do
    repo = Keyword.get(state.app_state_opts, :repo)

    state.app_name
    |> ShardLeases.get_owner_with_most_leases(state.stream_name, repo)
    |> case do
      nil ->
        []

      worker ->
        AppState.get_leases_by_worker(
          state.app_name,
          state.stream_name,
          worker,
          state.app_state_opts
        )
    end
  end

  @spec calculate_load_metrics(map()) :: map()
  def calculate_load_metrics(state) do
    %{
      state
      | target_load: calculate_target_load(state),
        current_load: get_current_worker_load(state)
    }
  end

  @spec total_leases_count_by_worker(map()) :: list({String.t(), integer})
  def total_leases_count_by_worker(state) do
    state.app_name
    |> AppState.total_incomplete_lease_counts_by_worker(
      state.stream_name,
      state.app_state_opts
    )
    |> then(fn worker_counts ->
      worker_counts
      |> Enum.find(fn {owner, _count} -> owner == state.lease_owner end)
      |> case do
        nil -> [worker_counts ++ {state.lease_owner, 0}]
        _ -> worker_counts
      end
    end)
  end

  @spec check_if_balanced?(map()) :: boolean()
  def check_if_balanced?(state) do
    state.app_name
    |> AppState.get_leases_by_worker(state.stream_name, state.lease_owner, state.app_state_opts)
    |> length()
    |> case do
      0 ->
        false

      _ ->
        all_workers_have_balanced_load?(state)
    end
  end

  defp all_workers_have_balanced_load?(state) do
    state.app_name
    |> AppState.total_incomplete_lease_counts_by_worker(
      state.stream_name,
      state.app_state_opts
    )
    |> Enum.all?(fn {_owner, count} ->
      abs(count - calculate_target_load(state)) <= 1
    end)
  end

  defp total_workers_count(workers, current_worker) do
    workers
    |> Enum.find(fn {owner, _count} -> owner == current_worker end)
    |> case do
      nil -> length(workers) + 1
      _ -> length(workers)
    end
  end

  defp calculate_target_load(state) do
    incomplete_leases_count =
      AppState.all_incomplete_leases(state.app_name, state.stream_name, state.app_state_opts)

    workers_count =
      AppState.total_incomplete_lease_counts_by_worker(
        state.app_name,
        state.stream_name,
        state.app_state_opts
      )

    total_shards = length(incomplete_leases_count)
    total_workers = total_workers_count(workers_count, state.lease_owner)

    if total_workers > 0, do: ceil(total_shards / total_workers), else: 0
  end

  defp get_current_worker_load(state) do
    state.app_name
    |> AppState.get_leases_by_worker(
      state.stream_name,
      state.lease_owner,
      state.app_state_opts
    )
    |> case do
      [] -> 0
      leases -> length(leases)
    end
  end
end
