defmodule KinesisClient.Stream.AppState.Ecto.CreateShardLeaseTable do
  @moduledoc false
  use Ecto.Migration

  def up do
    execute(
      "CREATE TABLE IF NOT EXISTS shard_lease (shard_id VARCHAR(255) PRIMARY KEY, checkpoint VARCHAR(255), lease_owner VARCHAR(255), lease_count INTEGER, completed BOOLEAN)"
    )

    create_if_not_exists(unique_index(:shard_lease, [:shard_id]))
  end

  def version, do: 20_240_304_000_001
end
