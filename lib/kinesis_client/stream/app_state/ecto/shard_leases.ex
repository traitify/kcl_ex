defmodule KinesisClient.Stream.AppState.Ecto.ShardLeases do
  @moduledoc false
  alias KinesisClient.Stream.AppState.Ecto.ShardLease, as: ShardLeaseEcto
  alias KinesisClient.Stream.AppState.ShardLease

  import Ecto.Query

  @spec get_shard_lease(map, Ecto.Repo.t()) ::
          {:error, :not_found} | {:error, :missing_required_fields} | {:ok, ShardLease.t()}
  def get_shard_lease(
        %{shard_id: _shard_id, app_name: _app_name, stream_name: _stream_name} = params,
        repo
      ) do
    ShardLeaseEcto.query()
    |> ShardLeaseEcto.build_get_query(params)
    |> repo.one()
    |> case do
      nil -> {:error, :not_found}
      shard_lease -> {:ok, shard_lease}
    end
  end

  def get_shard_lease(_params, _repo), do: {:error, :missing_required_fields}

  @spec get_shard_leases(any(), atom()) :: [] | [ShardLease.t()]
  def get_shard_leases(params, repo) do
    ShardLeaseEcto.query()
    |> ShardLeaseEcto.build_get_query(params)
    |> repo.all()
  end

  @spec insert_shard_lease(map, Ecto.Repo.t()) ::
          {:error, Ecto.Changeset.t()} | {:ok, ShardLease.t()}
  def insert_shard_lease(attrs, repo) do
    %ShardLeaseEcto{}
    |> ShardLeaseEcto.changeset(attrs)
    |> repo.insert()
  end

  @spec update_shard_lease(ShardLease.t(), Ecto.Repo.t(), list) ::
          {:error, :update_unsuccessful} | {:ok, ShardLease.t()}
  def update_shard_lease(shard_lease, repo, change) do
    ShardLeaseEcto
    |> build_where_clause(shard_lease)
    |> maybe_take_lease(change)
    |> select([sl], sl)
    |> update([sl], set: ^change)
    |> repo.update_all([])
    |> case do
      {1, [shard_lease]} -> {:ok, shard_lease}
      {_, _} -> {:error, :update_unsuccessful}
    end
  end

  @doc """
  Writes a checkpoint for a shard the caller still owns.

  Unlike `update_shard_lease/3`, this is guarded on ownership only
  (shard_id + app_name + stream_name + lease_owner), not on `lease_count`.
  A checkpoint just needs to confirm the lease is still held by this owner;
  gating it on `lease_count` would make it lose a benign race with the
  concurrent lease renewals that bump `lease_count`, failing checkpoints for
  a lease this worker still holds. This mirrors the Dynamo adapter, whose
  `update_checkpoint` conditions only on `lease_owner`.
  """
  @spec update_checkpoint(map, String.t(), Ecto.Repo.t()) ::
          {:error, :update_unsuccessful} | {:ok, ShardLease.t()}
  def update_checkpoint(
        %{shard_id: _, app_name: _, stream_name: _, lease_owner: _} = params,
        checkpoint,
        repo
      ) do
    ShardLeaseEcto.query()
    |> ShardLeaseEcto.build_get_query(params)
    |> select([sl], sl)
    |> update([sl], set: [checkpoint: ^checkpoint])
    |> repo.update_all([])
    |> case do
      {1, [shard_lease]} -> {:ok, shard_lease}
      {_, _} -> {:error, :update_unsuccessful}
    end
  end

  # Pins the row to the freshly read lease_count AND lease_owner so a
  # concurrent renew/take between our read and this update makes the
  # update_all match zero rows instead of clobbering the other worker's
  # lease. This matches the Dynamo adapter's conditional expressions.
  defp build_where_clause(query, shard_lease) do
    query
    |> where(
      [sl],
      sl.shard_id == ^shard_lease.shard_id and
        sl.app_name == ^shard_lease.app_name and
        sl.stream_name == ^shard_lease.stream_name and
        sl.lease_count == ^shard_lease.lease_count and
        sl.lease_owner == ^shard_lease.lease_owner
    )
  end

  defp maybe_take_lease(query, lease_owner: new_lease_owner) do
    query
    |> where([sl], sl.lease_owner != ^new_lease_owner)
  end

  defp maybe_take_lease(query, _), do: query
end
