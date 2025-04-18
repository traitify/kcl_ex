defmodule KinesisClient.Stream.AppState.Ecto.UpdateShardLeasePrimaryKey do
  use Ecto.Migration

  def up do
    # step 0: Drop the existing unique index if it exists
    execute("DROP INDEX IF EXISTS shard_lease_composite_unique;")

    # Step 1: Ensure app_name and stream_name are NOT NULL
    execute("ALTER TABLE shard_lease ALTER COLUMN app_name SET NOT NULL")
    execute("ALTER TABLE shard_lease ALTER COLUMN stream_name SET NOT NULL")

    # Step 2: Add a unique constraint to enforce uniqueness of the composite key
    execute(
      "CREATE UNIQUE INDEX shard_lease_composite_unique ON shard_lease (shard_id, app_name, stream_name)"
    )

    # Step 3: Drop the existing primary key constraint
    drop_if_exists(constraint(:shard_lease, "shard_lease_pkey"))

    # Step 4: Add the new composite primary key
    execute("ALTER TABLE shard_lease ADD PRIMARY KEY USING INDEX shard_lease_composite_unique")
  end

  def down do
    # Step 1: Drop the composite primary key
    execute("ALTER TABLE shard_lease DROP CONSTRAINT shard_lease_pkey")

    # Step 2: Recreate the original primary key on shard_id
    execute("ALTER TABLE shard_lease ADD PRIMARY KEY (shard_id)")

    # Step 3: Drop NOT NULL constraints to revert to original schema
    execute("ALTER TABLE shard_lease ALTER COLUMN app_name DROP NOT NULL")
    execute("ALTER TABLE shard_lease ALTER COLUMN stream_name DROP NOT NULL")
  end

  def version, do: 20_250_417_215_517
end
