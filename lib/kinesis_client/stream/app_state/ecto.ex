defmodule KinesisClient.Stream.AppState.Ecto do
  @moduledoc false

  @behaviour KinesisClient.Stream.AppState.Adapter

  alias KinesisClient.Stream.AppState.Ecto.AddAppAndStreamNameColumns
  alias KinesisClient.Stream.AppState.Ecto.CreateShardLeaseTable
  alias KinesisClient.Stream.AppState.Ecto.ShardLease
  alias KinesisClient.Stream.AppState.Ecto.ShardLeases

  require Logger

  @migrations [
    {CreateShardLeaseTable.version(), CreateShardLeaseTable},
    {AddAppAndStreamNameColumns.version(), AddAppAndStreamNameColumns}
  ]

  @impl true
  def initialize(app_name, opts) do
    with {:ok, repo} <- get_repo(opts),
         :ok <- run_migrations(repo) do
      backfill_app_name_and_stream_name_columns(repo, app_name, opts)
    end
  end

  @impl true
  def delete_all_leases_and_restart_workers(supervisor, __app_name, opts) do
    repo = Keyword.get(opts, :repo)

    with supervisor when not is_nil(supervisor) <- Process.whereis(supervisor),
         :ok <- repo.delete_all(ShardLease) do
      Logger.info("Shard leases deleted")
      Process.exit(supervisor, :shutdown)
      Logger.info("Restarting workers")

      {:ok, "Shard leases deleted and workers restarted"}
    else
      nil ->
        Logger.info("Supervisor not running")
        {:error, "Supervisor not running"}

      _ ->
        Logger.info("Failed to delete shard leases, reason unknown")
        {:error, "Failed to delete shard leases, reason unknown"}
    end
  end

  @impl true
  def create_lease(app_name, stream_name, shard_id, lease_owner, opts) do
    repo = Keyword.get(opts, :repo)

    attrs = %{
      shard_id: shard_id,
      app_name: app_name,
      stream_name: stream_name,
      lease_owner: lease_owner,
      completed: false,
      lease_count: 1
    }

    with {:ok, _} <- ShardLeases.insert_shard_lease(attrs, repo) do
      :ok
    else
      {:error, changeset} ->
        changeset
        |> extract_changeset_errors()
        |> already_exists()
    end
  end

  @impl true
  def get_lease(app_name, stream_name, shard_id, opts) do
    IO.inspect("ecto get_lease called")
    repo = Keyword.get(opts, :repo)
    IO.inspect(repo)
    shard_lease_params = %{shard_id: shard_id, app_name: app_name, stream_name: stream_name}
    IO.puts("shard_lease_paramss")
    IO.inspect(ShardLeases.get_shard_lease(shard_lease_params, repo))

    with {:ok, shard_lease} <- ShardLeases.get_shard_lease(shard_lease_params, repo) do
      shard_lease
    else
      {:error, :not_found} -> :not_found
    end
  end

  @impl true
  def renew_lease(
        app_name,
        stream_name,
        %{shard_id: shard_id, lease_owner: lease_owner, lease_count: lease_count},
        opts
      ) do
    repo = Keyword.get(opts, :repo)

    updated_count = lease_count + 1

    shard_lease_params = %{
      shard_id: shard_id,
      app_name: app_name,
      stream_name: stream_name,
      lease_owner: lease_owner,
      lease_count: lease_count
    }

    with {:ok, shard_lease} <- ShardLeases.get_shard_lease(shard_lease_params, repo),
         {:ok, _} <- ShardLeases.update_shard_lease(shard_lease, repo, lease_count: updated_count) do
      {:ok, updated_count}
    else
      {:error, _} -> {:error, :lease_renew_failed}
    end
  end

  @impl true
  def take_lease(app_name, stream_name, shard_id, new_lease_owner, lease_count, opts) do
    repo = Keyword.get(opts, :repo)

    updated_count = lease_count + 1

    shard_lease_params = %{
      shard_id: shard_id,
      app_name: app_name,
      stream_name: stream_name,
      lease_count: lease_count
    }

    with {:ok, shard_lease} <- ShardLeases.get_shard_lease(shard_lease_params, repo),
         {:ok, true} <- lease_owner_not_match(shard_lease, new_lease_owner),
         {:ok, _} <-
           ShardLeases.update_shard_lease(shard_lease, repo,
             lease_owner: new_lease_owner,
             lease_count: updated_count
           ) do
      {:ok, updated_count}
    else
      {:error, _} -> {:error, :lease_take_failed}
    end
  end

  @impl true
  def update_checkpoint(app_name, stream_name, shard_id, lease_owner, checkpoint, opts) do
    repo = Keyword.get(opts, :repo)

    shard_lease_params = %{
      shard_id: shard_id,
      app_name: app_name,
      stream_name: stream_name,
      lease_owner: lease_owner
    }

    with {:ok, shard_lease} <- ShardLeases.get_shard_lease(shard_lease_params, repo),
         {:ok, _} <- ShardLeases.update_shard_lease(shard_lease, repo, checkpoint: checkpoint) do
      :ok
    else
      {:error, _} -> {:error, :update_checkpoint_failed}
    end
  end

  @impl true
  def close_shard(app_name, stream_name, shard_id, lease_owner, opts) do
    repo = Keyword.get(opts, :repo)

    shard_lease_params = %{
      shard_id: shard_id,
      app_name: app_name,
      stream_name: stream_name,
      lease_owner: lease_owner
    }

    with {:ok, shard_lease} <- ShardLeases.get_shard_lease(shard_lease_params, repo),
         {:ok, _} <- ShardLeases.update_shard_lease(shard_lease, repo, completed: true) do
      :ok
    else
      {:error, _} -> {:error, :close_shard_failed}
    end
  end

  defp get_repo(opts), do: {:ok, Keyword.get(opts, :repo)}

  defp run_migrations(repo, migrations \\ @migrations) do
    Enum.each(migrations, fn {version, module} -> Ecto.Migrator.up(repo, version, module) end)
  end

  defp backfill_app_name_and_stream_name_columns(repo, app_name, opts) do
    stream_name = Keyword.get(opts, :stream_name)
    params = %{app_name: nil, stream_name: nil}

    ShardLease.query()
    |> ShardLease.build_get_query(params)
    |> repo.update_all(set: [app_name: app_name, stream_name: stream_name])

    :ok
  end

  defp already_exists(%{shard_id: ["has already been taken"]}), do: :already_exists
  defp already_exists(error), do: {:error, error}

  defp lease_owner_not_match(%{lease_owner: lease_owner}, new_lease_owner)
       when lease_owner == new_lease_owner,
       do: {:error, :lease_owner_match}

  defp lease_owner_not_match(%{lease_owner: _lease_owner}, _new_lease_owner), do: {:ok, true}

  defp extract_changeset_errors(changeset) do
    Ecto.Changeset.traverse_errors(changeset, fn {msg, opts} ->
      Regex.replace(~r"%{(\w+)}", msg, fn _, key ->
        opts |> Keyword.get(String.to_existing_atom(key), key) |> to_string()
      end)
    end)
  end
end
