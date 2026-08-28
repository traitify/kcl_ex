defmodule KinesisClient.Stream.AppState.EctoIntegrationTest do
  # Regression tests for TD-6470: initialize/2 crash-looped on any database
  # whose shard_lease table predates the app_name/stream_name columns, because
  # the NOT NULL migration ran before the backfill could populate legacy rows.
  #
  # Requires a running Postgres (see KinesisClient.Test.PostgresRepo for the
  # connection env vars). Run with: mix test --include integration
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias KinesisClient.Stream.AppState.Ecto, as: AppStateEcto
  alias KinesisClient.Test.PostgresRepo

  @moduletag :integration

  @app_name "assessment_service"
  @stream_name "profile-event-test"

  setup do
    config = PostgresRepo.default_config()
    _ = Ecto.Adapters.Postgres.storage_up(config)
    start_supervised!({PostgresRepo, config})

    PostgresRepo.query!("DROP TABLE IF EXISTS shard_lease")
    PostgresRepo.query!("DROP TABLE IF EXISTS schema_migrations")

    :ok
  end

  test "initialize/2 recovers a legacy table: backfills names, keeps checkpoints" do
    create_legacy_table()
    insert_legacy_row("shardId-000000000000", "#{@stream_name}-worker-42", "cp-0")
    insert_legacy_row("shardId-000000000001", "#{@stream_name}-worker-43", "cp-1")

    assert AppStateEcto.initialize(@app_name, repo: PostgresRepo, stream_name: @stream_name) == :ok

    rows = select_leases()
    assert map_size(rows) == 2

    assert rows["shardId-000000000000"] == {@app_name, @stream_name, "cp-0"}
    assert rows["shardId-000000000001"] == {@app_name, @stream_name, "cp-1"}

    assert names_not_null?()

    # A second boot against the migrated table must also succeed
    assert AppStateEcto.initialize(@app_name, repo: PostgresRepo, stream_name: @stream_name) == :ok
  end

  test "initialize/2 succeeds on a fresh database" do
    assert AppStateEcto.initialize(@app_name, repo: PostgresRepo, stream_name: @stream_name) == :ok
    assert names_not_null?()
  end

  test "backfills half-populated rows (OR, not AND)" do
    create_legacy_table()

    insert_legacy_row("shardId-000000000000", "#{@stream_name}-worker-1", "cp-half",
      app_name: @app_name
    )

    assert AppStateEcto.initialize(@app_name, repo: PostgresRepo, stream_name: @stream_name) == :ok

    assert select_leases() == %{"shardId-000000000000" => {@app_name, @stream_name, "cp-half"}}
  end

  test "backfills rows with the pre-#38 owner format (no stream prefix)" do
    create_legacy_table()
    insert_legacy_row("shardId-000000000000", "worker-1234", "cp-old-format")

    assert AppStateEcto.initialize(@app_name, repo: PostgresRepo, stream_name: @stream_name) == :ok

    assert select_leases() == %{
             "shardId-000000000000" => {@app_name, @stream_name, "cp-old-format"}
           }
  end

  test "deletes orphaned rows owned by no configured stream, with a warning" do
    create_legacy_table()
    insert_legacy_row("shardId-000000000000", "#{@stream_name}-worker-1", "cp-mine")
    insert_legacy_row("shardId-000000000009", "some-other-stream-worker-7", "cp-other")

    log =
      capture_log(fn ->
        assert AppStateEcto.initialize(@app_name, repo: PostgresRepo, stream_name: @stream_name) ==
                 :ok
      end)

    assert select_leases() == %{"shardId-000000000000" => {@app_name, @stream_name, "cp-mine"}}
    assert log =~ "deleted 1 orphaned shard_lease row(s)"
    assert log =~ "shardId-000000000009"
    assert log =~ "some-other-stream-worker-7"
  end

  test "LIKE metacharacters in the stream name do not over-match" do
    create_legacy_table()
    insert_legacy_row("shardId-000000000000", "my_stream-worker-1", "cp-exact")
    insert_legacy_row("shardId-000000000001", "myXstream-worker-1", "cp-wildcard")

    capture_log(fn ->
      assert AppStateEcto.initialize(@app_name, repo: PostgresRepo, stream_name: "my_stream") == :ok
    end)

    # The "_" in the stream name must match literally: myXstream is an orphan
    assert select_leases() == %{"shardId-000000000000" => {@app_name, "my_stream", "cp-exact"}}
  end

  defp create_legacy_table do
    Enum.each(AppStateEcto.pre_backfill_migrations(), fn {version, module} ->
      Ecto.Migrator.up(PostgresRepo, version, module, log: false)
    end)
  end

  defp insert_legacy_row(shard_id, lease_owner, checkpoint, opts \\ []) do
    PostgresRepo.query!(
      "INSERT INTO shard_lease (shard_id, checkpoint, lease_owner, lease_count, completed, app_name) VALUES ($1, $2, $3, 1, false, $4)",
      [shard_id, checkpoint, lease_owner, Keyword.get(opts, :app_name)]
    )
  end

  defp select_leases do
    %{rows: rows} =
      PostgresRepo.query!("SELECT shard_id, app_name, stream_name, checkpoint FROM shard_lease")

    Map.new(rows, fn [shard_id, app_name, stream_name, checkpoint] ->
      {shard_id, {app_name, stream_name, checkpoint}}
    end)
  end

  defp names_not_null? do
    %{rows: [[count]]} =
      PostgresRepo.query!(
        "SELECT count(*) FROM pg_attribute WHERE attrelid = 'shard_lease'::regclass AND attname IN ('app_name', 'stream_name') AND attnotnull"
      )

    count == 2
  end
end
