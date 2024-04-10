defmodule KinesisClient.Stream.AppState.ShardLease do
  @moduledoc false
  @derive ExAws.Dynamo.Encodable

  defstruct [
    :shard_id,
    :app_name,
    :stream_name,
    :checkpoint,
    :lease_owner,
    :lease_count,
    completed: false
  ]
end
