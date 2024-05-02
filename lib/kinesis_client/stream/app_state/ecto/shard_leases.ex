defmodule KinesisClient.Stream.AppState.Ecto.ShardLeases do

  alias KinesisClient.Stream.AppState.Ecto.ShardLease

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
    get_shard_lease(%{shard_id: shard_id}, repo)
  end

  @spec insert_shard_lease(map, Ecto.Repo.t()) ::
          {:error, Ecto.Changeset.t()} | {:ok, ShardLease.t()}
  def insert_shard_lease(attrs, repo) do
    %ShardLease{}
    |> ShardLease.changeset(attrs)
    |> repo.insert()
  end

  @spec update_shard_lease(ShardLease.t(), Ecto.Repo.t(), list) ::
          {:error, Ecto.Changeset.t()} | {:ok, ShardLease.t()}
  def update_shard_lease(shard_lease, repo, change \\ []) do
    shard_lease_changeset = Ecto.Changeset.change(shard_lease, change)

    case repo.update(shard_lease_changeset) do
      {:ok, shard_lease} -> {:ok, shard_lease}
      {:error, changeset} -> {:error, changeset}
    end
  end

  @spec delete_all_shard_leases_and_restart_workers(
          Ecto.Repo.t(),
          Supervisor.t() :: true | {:error, <<_::176>>}
        )
  def delete_all_shard_leases_and_restart_workers(repo, supervisor) do
    with supervisor when not is_nil(supervisor) <- Process.whereis(supervisor),
         :ok <- repo.delete_all(ShardLease) do
      Process.exit(supervisor, :shutdown)
    else
      nil -> {:error, "Supervisor not running"}
    end
  end
end
