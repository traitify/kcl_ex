defmodule KinesisClient.Stream.Shard.LoadBalance do
  @moduledoc """
  Pure decision logic for balancing shard leases across workers.

  Given the incomplete-lease counts per worker, decides whether the current
  worker should steal a lease from an overloaded worker. Fetching the counts
  and executing the steal are the caller's concern (see
  `KinesisClient.Stream.Rebalancer`).
  """

  @doc """
  Decides whether `lease_owner` should steal a lease.

  Returns `{:steal_from, victim, deficit}` when `lease_owner` holds less than
  its share of the leases (`ceil(total_leases / total_workers)`) and the most
  loaded worker leads it by more than one lease, so a steal moves the
  distribution closer to even instead of flipping the imbalance around.
  `deficit` is how many leases `lease_owner` is short of the target — the
  caller must not steal more than that in one round, or the imbalance flips
  to the other side and oscillates. Returns `:balanced` otherwise.

  `lease_owner` is counted as a worker even when it holds no leases and is
  therefore absent from the grouped counts — otherwise a fresh worker would
  look "balanced" and never claim its share of the shards.
  """
  @spec decide(list({String.t(), non_neg_integer()}), String.t()) ::
          :balanced | {:steal_from, String.t(), pos_integer()}
  def decide(worker_counts, lease_owner) do
    worker_counts = include_current_worker(worker_counts, lease_owner)
    target = target_load(worker_counts)
    {^lease_owner, my_count} = List.keyfind(worker_counts, lease_owner, 0)

    worker_counts
    |> List.keydelete(lease_owner, 0)
    |> steal_candidate(my_count, target)
  end

  defp steal_candidate([], _my_count, _target), do: :balanced

  defp steal_candidate(other_counts, my_count, target) do
    other_counts
    |> Enum.max_by(fn {_owner, count} -> count end)
    |> case do
      {victim, victim_count} when my_count < target and victim_count - my_count > 1 ->
        {:steal_from, victim, target - my_count}

      _ ->
        :balanced
    end
  end

  defp include_current_worker(worker_counts, lease_owner) do
    worker_counts
    |> List.keymember?(lease_owner, 0)
    |> case do
      true -> worker_counts
      false -> [{lease_owner, 0} | worker_counts]
    end
  end

  defp target_load(worker_counts) do
    total_leases = worker_counts |> Enum.map(fn {_owner, count} -> count end) |> Enum.sum()

    ceil(total_leases / length(worker_counts))
  end
end
