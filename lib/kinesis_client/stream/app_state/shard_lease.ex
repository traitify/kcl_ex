defmodule KinesisClient.Stream.AppState.ShardLease do
  @moduledoc false
  @derive ExAws.Dynamo.Encodable

  @type t :: %__MODULE__{
          shard_id: String.t(),
          app_name: String.t(),
          stream_name: String.t(),
          checkpoint: String.t(),
          lease_owner: String.t(),
          lease_count: integer(),
          completed: boolean()
        }

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
