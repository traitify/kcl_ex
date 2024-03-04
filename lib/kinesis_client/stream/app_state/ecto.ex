defmodule KinesisClient.Stream.AppState.Ecto do
  @moduledoc false

  @behaviour KinesisClient.Stream.AppState.Adapter

  alias KinesisClient.Stream.AppState.Ecto.Migration
  alias KinesisClient.Stream.AppState.Ecto.NewColumnsMigration
  alias KinesisClient.Stream.AppState.Ecto.ShardLeases

  @impl true
  def initialize(_app_name, opts) do
    repo = Keyword.get(opts, :repo)

    # NOTE: Uncomment the following lines after successfully
    # updating assessment_service shard_lease table and running backfill,
    # and also remove the NewColumnsMigration and its references.
    #
    # case Ecto.Migrator.up(repo, version(), Migration) do
    #   :ok -> :ok
    #   :already_up -> :ok
    # end

    with :ok <- run_migrator(repo, Migration) do
      Process.sleep(1000)

      run_migrator(repo, NewColumnsMigration)
    end
  end

  defp run_migrator(repo, module) do
    case Ecto.Migrator.up(repo, version(), module) do
      :ok -> :ok
      :already_up -> :ok
    end
  end

  @impl true
  def create_lease(_app_name, shard_id, lease_owner, opts) do
    repo = Keyword.get(opts, :repo)

    attrs = %{
      shard_id: shard_id,
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
  def get_lease(_app_name, shard_id, opts) do
    repo = Keyword.get(opts, :repo)

    with {:ok, shard_lease} <- ShardLeases.get_shard_lease_by_id(shard_id, repo) do
      shard_lease
    else
      {:error, :not_found} -> :not_found
    end
  end

  @impl true
  def renew_lease(
        _app_name,
        %{shard_id: shard_id, lease_owner: lease_owner, lease_count: lease_count},
        opts
      ) do
    repo = Keyword.get(opts, :repo)

    updated_count = lease_count + 1

    shard_lease_params = %{shard_id: shard_id, lease_owner: lease_owner, lease_count: lease_count}

    with {:ok, shard_lease} <- ShardLeases.get_shard_lease(shard_lease_params, repo),
         {:ok, _} <- ShardLeases.update_shard_lease(shard_lease, repo, lease_count: updated_count) do
      {:ok, updated_count}
    else
      {:error, _} -> {:error, :lease_renew_failed}
    end
  end

  @impl true
  def take_lease(_app_name, shard_id, new_lease_owner, lease_count, opts) do
    repo = Keyword.get(opts, :repo)

    updated_count = lease_count + 1

    shard_lease_params = %{shard_id: shard_id, lease_count: lease_count}

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
  def update_checkpoint(_app_name, shard_id, lease_owner, checkpoint, opts) do
    repo = Keyword.get(opts, :repo)

    shard_lease_params = %{shard_id: shard_id, lease_owner: lease_owner}

    with {:ok, shard_lease} <- ShardLeases.get_shard_lease(shard_lease_params, repo),
         {:ok, _} <- ShardLeases.update_shard_lease(shard_lease, repo, checkpoint: checkpoint) do
      :ok
    else
      {:error, _} -> {:error, :update_checkpoint_failed}
    end
  end

  @impl true
  def close_shard(_app_name, shard_id, lease_owner, opts) do
    repo = Keyword.get(opts, :repo)

    shard_lease_params = %{shard_id: shard_id, lease_owner: lease_owner}

    with {:ok, shard_lease} <- ShardLeases.get_shard_lease(shard_lease_params, repo),
         {:ok, _} <- ShardLeases.update_shard_lease(shard_lease, repo, completed: true) do
      :ok
    else
      {:error, _} -> {:error, :close_shard_failed}
    end
  end

  defp version do
    DateTime.utc_now()
    |> Calendar.strftime("%Y%m%d%H%M%S")
    |> String.to_integer()
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
