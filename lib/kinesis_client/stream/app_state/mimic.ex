defmodule KinesisClient.Stream.AppState.Mimic do
  @moduledoc false

  alias KinesisClient.Stream.AppState.Dynamo
  alias KinesisClient.Stream.AppState.Ecto

  @behaviour KinesisClient.Stream.AppState.Adapter

  require Logger

  @impl true
  def initialize(app_name, opts) do
    {from, to} = modules(opts)

    to.initialize(app_name, opts)
    from.initialize(app_name, opts)
  end

  @impl true
  def get_lease(app_name, stream_name, shard_id, opts) do
    {from, to} = modules(opts)

    app_name
    |> to.get_lease(stream_name, shard_id, opts)
    |> case do
      {:error, error} ->
        Logger.error(
          "Error getting lease for shard #{shard_id} during migration: #{inspect(error)}"
        )

        from.get_lease(app_name, stream_name, shard_id, opts)

      :not_found ->
        from.get_lease(app_name, stream_name, shard_id, opts)
        |> then(fn lease ->
          maybe_create_lease(to, lease, opts)
        end)

      _lease ->
        from.get_lease(app_name, stream_name, shard_id, opts)
    end
  end

  @impl true
  def get_leases_by_worker(_app_name, _stream_name, _lease_owner, _opts) do
    []
  end

  @impl true
  def create_lease(app_name, stream_name, shard_id, lease_owner, opts) do
    {from, to} = modules(opts)

    to.create_lease(app_name, stream_name, shard_id, lease_owner, opts)
    from.create_lease(app_name, stream_name, shard_id, lease_owner, opts)
  end

  @impl true
  def update_checkpoint(app_name, stream_name, shard_id, lease_owner, checkpoint, opts) do
    {from, to} = modules(opts)

    to.update_checkpoint(app_name, stream_name, shard_id, lease_owner, checkpoint, opts)
    from.update_checkpoint(app_name, stream_name, shard_id, lease_owner, checkpoint, opts)
  end

  @impl true
  def renew_lease(app_name, stream_name, shard_lease, opts) do
    {from, to} = modules(opts)

    to.renew_lease(app_name, stream_name, shard_lease, opts)
    from.renew_lease(app_name, stream_name, shard_lease, opts)
  end

  @impl true
  def take_lease(app_name, stream_name, shard_id, new_owner, lease_count, opts) do
    {from, to} = modules(opts)

    to.take_lease(app_name, stream_name, shard_id, new_owner, lease_count, opts)
    from.take_lease(app_name, stream_name, shard_id, new_owner, lease_count, opts)
  end

  @impl true
  def close_shard(app_name, stream_name, shard_id, lease_owner, opts) do
    {from, to} = modules(opts)

    to.close_shard(app_name, stream_name, shard_id, lease_owner, opts)
    from.close_shard(app_name, stream_name, shard_id, lease_owner, opts)
  end

  @impl true
  def all_incomplete_leases(app_name, stream_name, opts) do
    {from, to} = modules(opts)

    to.all_incomplete_leases(app_name, stream_name, opts)
    from.all_incomplete_leases(app_name, stream_name, opts)
  end

  @impl true
  def total_incomplete_lease_counts_by_worker(app_name, stream_name, opts) do
    {from, to} = modules(opts)

    to.total_incomplete_lease_counts_by_worker(app_name, stream_name, opts)
    from.total_incomplete_lease_counts_by_worker(app_name, stream_name, opts)
  end

  defp modules(opts) do
    migration = Keyword.get(opts, :migration)

    {get_module(Keyword.get(migration, :from)), get_module(Keyword.get(migration, :to))}
  end

  defp get_module(:dynamo), do: Dynamo
  defp get_module(:ecto), do: Ecto

  defp maybe_create_lease(_to_module, :not_found, _opts), do: :not_found
  defp maybe_create_lease(_to_module, {:error, _} = error, _opts), do: error

  defp maybe_create_lease(to_module, lease, opts) do
    %{
      shard_id: lease.shard_id,
      checkpoint: lease.checkpoint,
      app_name: get_app_name(opts),
      stream_name: get_stream_name(opts),
      lease_owner: lease.lease_owner,
      completed: lease.completed,
      lease_count: lease.lease_count
    }
    |> to_module.create_lease(opts)
    |> case do
      :ok ->
        lease

      error ->
        Logger.error("Error creating lease from mimic during migration: #{inspect(error)}")
        lease
    end
  end

  defp get_app_name(opts), do: Keyword.get(opts, :app_name)
  defp get_stream_name(opts), do: Keyword.get(opts, :stream_name)
end
