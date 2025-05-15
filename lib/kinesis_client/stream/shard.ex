defmodule KinesisClient.Stream.Shard do
  @moduledoc false
  use Supervisor, restart: :transient

  import KinesisClient.Util

  alias KinesisClient.Stream.Shard.Lease
  alias KinesisClient.Stream.Shard.Pipeline

  def start_link(args) do
    Supervisor.start_link(__MODULE__, args, name: args[:shard_name])
  end

  def init(opts) do
    lease_opts =
      [
        app_name: opts[:app_name],
        stream_name: opts[:stream_name],
        shard_id: opts[:shard_id],
        lease_owner: opts[:lease_owner]
      ]
      |> optional_kw(:app_state_opts, Keyword.get(opts, :app_state_opts))
      |> optional_kw(:renew_interval, Keyword.get(opts, :lease_renew_interval))
      |> optional_kw(:lease_expiry, Keyword.get(opts, :lease_expiry))
      |> optional_kw(:pipeline, Keyword.get(opts, :pipeline))
      |> optional_kw(:lease_renewal_limit, Keyword.get(opts, :lease_renewal_limit))
      |> optional_kw(:spread_lease, Keyword.get(opts, :spread_lease))

    pipeline_opts =
      [
        app_state_opts: opts[:app_state_opts],
        app_name: opts[:app_name],
        shard_id: opts[:shard_id],
        lease_owner: opts[:lease_owner],
        stream_name: opts[:stream_name],
        shard_consumer: opts[:shard_consumer],
        processors: opts[:processors],
        batchers: opts[:batchers],
        coordinator_name: opts[:coordinator_name]
      ]
      |> optional_kw(:poll_interval, Keyword.get(opts, :poll_interval))

    children = [
      {Lease, lease_opts},
      {Pipeline, pipeline_opts}
    ]

    Supervisor.init(children, strategy: :one_for_all)
  end

  def start(supervisor, shard_info) do
    DynamicSupervisor.start_child(supervisor, {__MODULE__, shard_info})
  end

  def stop(shard) do
    Supervisor.stop(shard, :normal)
  end
end
