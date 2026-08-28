defmodule KinesisClient.Stream.AppState.Ecto do
  @moduledoc false

  @behaviour KinesisClient.Stream.AppState.Adapter

  alias KinesisClient.Stream.AppState.Ecto.AddAdditionalUniqueConstraints
  alias KinesisClient.Stream.AppState.Ecto.AddAppAndStreamNameColumns
  alias KinesisClient.Stream.AppState.Ecto.CreateShardLeaseTable
  alias KinesisClient.Stream.AppState.Ecto.UpdateShardLeasePrimaryKey
  alias KinesisClient.Stream.AppState.Ecto.ShardLease
  alias KinesisClient.Stream.AppState.Ecto.ShardLeases

  @pre_backfill_migrations [
    {CreateShardLeaseTable.version(), CreateShardLeaseTable},
    {AddAppAndStreamNameColumns.version(), AddAppAndStreamNameColumns},
    {AddAdditionalUniqueConstraints.version(), AddAdditionalUniqueConstraints}
  ]

  # UpdateShardLeasePrimaryKey sets app_name/stream_name NOT NULL, so it must
  # not run until the backfill below has populated rows written by older
  # library versions, or it raises 23502 and crash-loops the coordinator.
  @post_backfill_migrations [
    {UpdateShardLeasePrimaryKey.version(), UpdateShardLeasePrimaryKey}
  ]

  require Logger

  # Public so tests can build a faithful legacy (pre-backfill) table shape.
  def pre_backfill_migrations, do: @pre_backfill_migrations

  @impl true
  def initialize(app_name, opts) do
    with {:ok, repo} <- get_repo(opts),
         :ok <- run_migrations(repo, @pre_backfill_migrations),
         :ok <- backfill_app_name_and_stream_name_columns(repo, app_name, opts),
         :ok <- delete_orphaned_leases(repo) do
      run_migrations(repo, @post_backfill_migrations)
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
    repo = Keyword.get(opts, :repo)
    shard_lease_params = %{shard_id: shard_id, app_name: app_name, stream_name: stream_name}

    with {:ok, shard_lease} <- ShardLeases.get_shard_lease(shard_lease_params, repo) do
      shard_lease
    else
      {:error, :not_found} -> :not_found
    end
  end

  @impl true
  def get_leases_by_worker(app_name, stream_name, lease_owner, opts) do
    repo = Keyword.get(opts, :repo)

    %{
      lease_owner: lease_owner,
      app_name: app_name,
      stream_name: stream_name
    }
    |> ShardLeases.get_shard_leases(repo)
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
      {:error, error} ->
        Logger.error(
          "KinesisClient: Error trying to renew lease for #{shard_id}: #{inspect(error)}"
        )

        {:error, :lease_renew_failed}
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
      # A lost optimistic-lock race: the row was found, but lease_count or
      # lease_owner changed between the read and the conditional update. This
      # is the only genuine contention case here, and it is expected on every
      # scale-out and failover, so it stays at :warning.
      {:error, :update_unsuccessful} ->
        Logger.warning(
          "KinesisClient: Could not take lease for #{shard_id} " <>
            "(lost the optimistic-lock race, another worker took it first)"
        )

        {:error, :lease_take_failed}

      # No row matched (shard_id, app_name, stream_name, lease_count). The
      # lookup is keyed on lease_count, so this is a caller passing a
      # lease_count that does not match the stored row — not contention, and
      # not something a retry fixes on its own. Say so, because labelling it
      # contention hides a stale-read bug behind expected-looking noise.
      {:error, :not_found} ->
        Logger.warning(
          "KinesisClient: Could not take lease for #{shard_id}: no lease row matches " <>
            "lease_count #{inspect(lease_count)}. This is a stale or incorrect lease_count " <>
            "from the caller, or a missing lease row — not lease contention."
        )

        {:error, :lease_take_failed}

      # This worker already owns the lease, so there is nothing to take.
      {:error, :lease_owner_match} ->
        Logger.debug(
          "KinesisClient: Not taking lease for #{shard_id}, already owned by #{new_lease_owner}"
        )

        {:error, :lease_take_failed}

      {:error, :missing_required_fields} ->
        Logger.error(
          "KinesisClient: Could not take lease for #{shard_id}: lookup was missing required " <>
            "fields — #{inspect(shard_lease_params)}"
        )

        {:error, :lease_take_failed}

      {:error, error} ->
        Logger.warning("KinesisClient: Could not take lease for #{shard_id}: #{inspect(error)}")

        {:error, :lease_take_failed}
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

    case ShardLeases.update_checkpoint(shard_lease_params, checkpoint, repo) do
      {:ok, _} ->
        :ok

      {:error, error} ->
        Logger.error(
          "KinesisClient: Error trying to update checkpoint for #{shard_id}: #{inspect(error)}"
        )

        {:error, :update_checkpoint_failed}
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

  @impl true
  def all_incomplete_leases(app_name, stream_name, opts) do
    repo = Keyword.get(opts, :repo)

    %{
      app_name: app_name,
      stream_name: stream_name,
      completed: false
    }
    |> ShardLeases.get_shard_leases(repo)
  end

  def create_lease(attrs, opts) when is_map(attrs) do
    repo = Keyword.get(opts, :repo)

    with {:ok, _} <- ShardLeases.insert_shard_lease(attrs, repo) do
      :ok
    else
      {:error, changeset} ->
        changeset
        |> extract_changeset_errors()
        |> already_exists()
    end
  end

  defp get_repo(opts), do: {:ok, Keyword.get(opts, :repo)}

  defp run_migrations(repo, migrations) do
    Enum.each(migrations, fn {version, module} -> Ecto.Migrator.up(repo, version, module) end)
  end

  # Claims only legacy rows written by this stream's own workers (lease_owner
  # is "#{stream_name}-worker-#{n}"), so one stream cannot stamp its names
  # onto another stream's rows when several streams share a repo.
  defp backfill_app_name_and_stream_name_columns(repo, app_name, opts) do
    stream_name = Keyword.fetch!(opts, :stream_name)

    ShardLease.query()
    |> ShardLease.missing_names()
    |> ShardLease.owned_by_stream_workers(stream_name)
    |> repo.update_all(set: [app_name: app_name, stream_name: stream_name])

    :ok
  end

  # Rows the backfill did not claim (renamed streams, retired workers) would
  # still fail the NOT NULL migration, so they are deleted. If such a lease is
  # live, its consumer restarts from the stream's configured initial position.
  defp delete_orphaned_leases(repo) do
    {count, orphans} =
      ShardLease.query()
      |> ShardLease.missing_names()
      |> ShardLease.select_owner_info()
      |> repo.delete_all()

    log_orphaned_leases(count, orphans)
  end

  defp log_orphaned_leases(0, _orphans), do: :ok

  defp log_orphaned_leases(count, orphans) do
    Logger.warning(
      "KinesisClient: deleted #{count} orphaned shard_lease row(s) with no app_name/stream_name " <>
        "whose lease_owner matches no configured stream. Any live consumer of these shards will " <>
        "restart from the stream's configured initial position: #{inspect(orphans)}"
    )

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
