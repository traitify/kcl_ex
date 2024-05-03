defmodule KinesisClient.Stream.AppState.Adapter do
  @moduledoc false

  alias KinesisClient.Stream.AppState.Ecto.ShardLease, as: EctoShardLease
  alias KinesisClient.Stream.AppState.ShardLease, as: DynamoShardLease

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
              DynamoShardLease.t() | EctoShardLease.t() | :not_found | {:error, any}

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
              shard_lease :: DynamoShardLease.t() | EctoShardLease.t(),
              opts :: keyword
            ) ::
              {:ok, new_lease_count :: integer} | :lease_renew_failed | {:error, any}

  @callback take_lease(
              app_name :: String.t(),
              stream_name :: String.t(),
              shard_id :: String.t(),
              new_owner :: String.t(),
              lease_count :: integer,
              opts :: keyword
            ) ::
              {:ok, new_lease_count :: integer} | :lease_take_failed | {:error, any}

  @callback close_shard(
              app_name :: String.t(),
              stream_name :: String.t(),
              shard_id :: String.t(),
              lease_owner :: String.t(),
              opts :: keyword
            ) ::
              :ok | {:error, any}

  @callback delete_all_shard_leases_and_restart_workers(opts :: keyword, supervisor :: Supervisor.t()) :: {:ok, any} | {:error, any}
end
