defmodule KinesisClient.Stream.AppState.PostgresTest do
  use ExUnit.Case

  alias KinesisClient.PostgresRepo
  alias KinesisClient.Stream.AppState.Postgres

  test "creates a shard_lease" do
    assert Postgres.create_lease("", "a.b.c", "test_owner", repo: PostgresRepo) == :ok
  end

  test "gets a shard_lease" do
    shard_lease = Postgres.get_lease("", "a.b.c", repo: PostgresRepo)

    assert shard_lease.shard_id == "a.b.c"
    assert shard_lease.checkpoint == nil
    assert shard_lease.completed == false
    assert shard_lease.lease_count == 1
    assert shard_lease.lease_owner == "test_owner"
  end

  test "renews a shard_lease" do
    change = %{
      shard_id: "a.b.c",
      lease_owner: "test_owner",
      lease_count: 1
    }

    {:ok, lease_count} = Postgres.renew_lease("", change, repo: PostgresRepo)

    assert lease_count == 2
  end

  test "takes a shard_lease" do
    {:ok, lease_count} = Postgres.take_lease("", "a.b.c", "new_owner", 1, repo: PostgresRepo)

    assert lease_count == 2
  end

  test "returns error when taking a shard_lease" do
    {:error, error} = Postgres.take_lease("", "a.b.c", "test_owner", 1, repo: PostgresRepo)

    assert error == :lease_take_failed
  end

  test "updates shard_lease checkpoint" do
    assert Postgres.update_checkpoint("", "a.b.c", "test_owner", "checkpoint_1", repo: PostgresRepo) ==
             :ok
  end

  test "closes shard" do
    assert Postgres.close_shard("", "a.b.c", "test_owner", repo: PostgresRepo) == :ok
  end
end
