defmodule KinesisClient.Stream.AppState do
  @moduledoc """
  The AppState is where the information about Stream shards are stored. ShardConsumers will
  checkpoint the records, and the `KinesisClient.Stream.Coordinator` will check here to determine
  what shards to consume.
  """

  def initialize(app_name, opts \\ []), do: adapter(opts).initialize(app_name, opts)

  @doc """
  Get a `KinesisClient.Stream.AppState.ShardInfo` struct by shard_id. If there is not an existing
  record, returns `:not_found`.
  """
  def get_lease(app_name, stream_name, shard_id, opts \\ []) do
    IO.inspect("app get_lease called")
    IO.inspect("app_name: #{app_name}, stream_name: #{stream_name}, shard_id: #{shard_id}")
    IO.puts("opts: #{inspect(opts)}")
    IO.puts("adapter: #{inspect(adapter(opts))}")
    adapter(opts).get_lease(app_name, stream_name, shard_id, opts)
  end

  @doc """
  Persists a new ShardInfo record. Returns an error if there is already a record for that `shard_id`
  """
  def create_lease(app_name, stream_name, shard_id, lease_owner, opts \\ []),
    do: adapter(opts).create_lease(app_name, stream_name, shard_id, lease_owner, opts)

  @doc """
  Update the checkpoint of the shard with the last sequence number that was processed by a
  ShardConsumer. Will return {:error, :lead_invalid} if the `lease` does not match what is in
  `ShardInfo` and the checkpoint will not be updated.
  """
  def update_checkpoint(app_name, stream_name, shard_id, lease, checkpoint, opts \\ []),
    do: adapter(opts).update_checkpoint(app_name, stream_name, shard_id, lease, checkpoint, opts)

  @doc """
  Renew lease. Increments :lease_count.
  """
  def renew_lease(app_name, stream_name, shard_lease, opts \\ []),
    do: adapter(opts).renew_lease(app_name, stream_name, shard_lease, opts)

  def take_lease(app_name, stream_name, shard_id, new_owner, lease_count, opts \\ []),
    do: adapter(opts).take_lease(app_name, stream_name, shard_id, new_owner, lease_count, opts)

  @doc """
  Marks a ShardLease as completed.

  This indicates that all records for the shard have been processed by the app. `KinesisClient.Stream.Shard`
  processes will not be started for ShardLease's that are completed.
  """
  def close_shard(app_name, stream_name, shard_id, opts \\ []),
    do: adapter(opts).close_shard(app_name, stream_name, shard_id, opts)

  defp adapter(opts) do
    case Keyword.get(opts, :adapter) do
      :ecto -> KinesisClient.Stream.AppState.Ecto
      :test -> KinesisClient.Stream.AppStateMock
      _ -> KinesisClient.Stream.AppState.Dynamo
    end
  end
end
