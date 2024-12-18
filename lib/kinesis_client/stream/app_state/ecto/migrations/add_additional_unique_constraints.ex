defmodule KinesisClient.Stream.AppState.Ecto.AddAdditionalUniqueConstraints do
  @moduledoc false
  use Ecto.Migration

  def up do
    drop_if_exists(unique_index(:shard_lease, [:shard_id]))

    create_if_not_exists(unique_index(:shard_lease, [:shard_id, :app_name, :stream_name]))
  end

  def version, do: 20_240_304_000_003
end
