defmodule KinesisClient.Stream.AppState.Adapter do
  @moduledoc false

  alias KinesisClient.Stream.AppState.ShardLease

  @doc """
  Implement to setup any backend storage. Should not clear data as this will be called everytime a
  `KinesisClient.Stream.Coordinator` process is started.
  """
  @callback initialize(app_name :: String.t(), opts :: keyword) :: :ok | {:error, any}

  @callback get_lease(
              app_name :: String.t(),
              stream_name :: String.t(),
              shard_id :: String.t(),
              opts :: keyword
            ) ::
              ShardLease.t() | :not_found | {:error, any}

  @callback get_leases_by_worker(
              app_name :: String.t(),
              stream_name :: String.t(),
              lease_owner :: String.t(),
              opts :: keyword
            ) ::
              list(ShardLease.t())

  @callback create_lease(
              app_name :: String.t(),
              stream_name :: String.t(),
              shard_id :: String.t(),
              lease_owner :: String.t(),
              opts :: keyword
            ) ::
              :ok | :already_exists | {:error, any}

  @callback update_checkpoint(
              app_name :: String.t(),
              stream_name :: String.t(),
              shard_id :: String.t(),
              lease_owner :: String.t(),
              checkpoint :: String.t(),
              opts :: keyword
            ) ::
              :ok | {:error, any}

  @callback renew_lease(
              app_name :: String.t(),
              stream_name :: String.t(),
              shard_lease :: ShardLease.t(),
              opts :: keyword
            ) ::
              {:ok, new_lease_count :: integer} | {:error, :lease_renew_failed} | {:error, any}

  @callback take_lease(
              app_name :: String.t(),
              stream_name :: String.t(),
              shard_id :: String.t(),
              new_owner :: String.t(),
              lease_count :: integer,
              opts :: keyword
            ) ::
              {:ok, new_lease_count :: integer} | {:error, :lease_take_failed} | {:error, any}

  @callback close_shard(
              app_name :: String.t(),
              stream_name :: String.t(),
              shard_id :: String.t(),
              lease_owner :: String.t(),
              opts :: keyword
            ) ::
              :ok | {:error, any}

  @callback all_incomplete_leases(
              app_name :: String.t(),
              stream_name :: String.t(),
              opts :: keyword
            ) ::
              list(ShardLease.t())

  @callback total_incomplete_lease_counts_by_worker(
              app_name :: String.t(),
              stream_name :: String.t(),
              opts :: keyword
            ) ::
              list({worker :: String.t(), count :: integer})

  @callback lease_owner_with_most_leases(
              app_name :: String.t(),
              stream_name :: String.t(),
              opts :: keyword
            ) ::
              String.t() | nil
end
