defmodule KinesisClient.Stream.AppState.Ecto.ShardLeases do
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

  @spec incomplete_group_by_owner(String.t(), String.t(), Ecto.Repo.t()) ::
          [{lease_owner :: String.t(), count :: integer}]
  def incomplete_group_by_owner(app_name, stream_name, repo) do
    from(sl in ShardLeaseEcto,
      where: sl.app_name == ^app_name and sl.stream_name == ^stream_name and not sl.completed,
      group_by: sl.lease_owner,
      select: {sl.lease_owner, count(sl.shard_id)}
    )
    |> repo.all()
  end

  @spec get_owner_with_most_leases(String.t(), String.t(), Ecto.Repo.t()) ::
          owner :: String.t() | nil
  def get_owner_with_most_leases(app_name, stream_name, repo) do
    owner_counts = incomplete_group_by_owner(app_name, stream_name, repo)

    case owner_counts do
      [] ->
        nil

      counts ->
        max_count = counts |> Enum.map(fn {_owner, count} -> count end) |> Enum.max()

        {owner, _count} = Enum.find(counts, fn {_owner, count} -> count == max_count end)

        owner
    end
  end

  defp build_where_clause(query, shard_lease) do
    query
    |> where(
      [sl],
      sl.shard_id == ^shard_lease.shard_id and
        sl.app_name == ^shard_lease.app_name and
        sl.stream_name == ^shard_lease.stream_name and
        sl.lease_count == ^shard_lease.lease_count
    )
  end

  defp maybe_take_lease(query, lease_owner: new_lease_owner) do
    query
    |> where([sl], sl.lease_owner != ^new_lease_owner)
  end

  defp maybe_take_lease(query, _), do: query
end
