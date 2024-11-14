defmodule KinesisClient.Stream.AppState.Ecto.ShardLeases do
  alias KinesisClient.Stream.AppState.Ecto.ShardLease

  import Ecto.Query

  @spec get_shard_lease(map, Ecto.Repo.t()) :: {:error, :not_found} | {:ok, ShardLease.t()}
  def get_shard_lease(params, repo) do
    ShardLease.query()
    |> ShardLease.build_get_query(params)
    |> repo.one()
    |> case do
      nil -> {:error, :not_found}
      shard_lease -> {:ok, shard_lease}
    end
  end

  @spec get_shard_lease_by_id(String.t(), Ecto.Repo.t()) ::
          {:error, :not_found} | {:ok, ShardLease.t()}
  def get_shard_lease_by_id(shard_id, repo) do
    %{shard_id: shard_id}
    |> get_shard_lease(repo)
  end

  @spec insert_shard_lease(map, Ecto.Repo.t()) ::
          {:error, Ecto.Changeset.t()} | {:ok, ShardLease.t()}
  def insert_shard_lease(attrs, repo) do
    %ShardLease{}
    |> ShardLease.changeset(attrs)
    |> repo.insert()
  end

  @spec update_shard_lease(ShardLease.t(), Ecto.Repo.t(), list) ::
          {:error, :update_unsuccessful} | {:ok, ShardLease.t()}
  def update_shard_lease(shard_lease, repo, change \\ []) do
    from(sl in ShardLease,
      where: sl.shard_id == ^shard_lease.shard_id and sl.lease_count == ^shard_lease.lease_count,
      select: sl,
      update: [set: ^change]
    )
    |> repo.update_all([])
    |> case do
      {1, [shard_lease]} -> {:ok, shard_lease}
      {_, _} -> {:error, :update_unsuccessful}
    end
  end
end
