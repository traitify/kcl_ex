defmodule KinesisClient.Stream.AppState.EctoTest do
  use ExUnit.Case

  alias KinesisClient.Ecto.Repo
  alias KinesisClient.Stream.AppState.Ecto
  alias KinesisClient.Stream.AppState.Ecto.ShardLease
  alias KinesisClient.Stream.AppState.Ecto.ShardLeases

  # Captures the query handed to update_all/2 so a test can assert on the
  # WHERE clause the checkpoint write is guarded by.
  defmodule CaptureRepo do
    def update_all(query, []) do
      send(self(), {:update_all, query})
      {1, [%ShardLease{checkpoint: "checkpoint_1"}]}
    end
  end

  # Simulates the conditional write matching no rows (lease no longer owned).
  defmodule NoRowRepo do
    def update_all(_query, []), do: {0, []}
  end

  test "creates a shard_lease" do
    assert Ecto.create_lease("", "stream_name", "a.b.c", "test_owner", repo: Repo) == :ok
  end

  test "gets a shard_lease" do
    shard_lease = Ecto.get_lease("app_name", "stream_name", "a.b.c", repo: Repo)

    assert shard_lease.shard_id == "a.b.c"
    assert shard_lease.app_name == "app_name"
    assert shard_lease.stream_name == "stream_name"
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

    {:ok, lease_count} = Ecto.renew_lease("app_name", "stream_name", change, repo: Repo)

    assert lease_count == 2
  end

  test "takes a shard_lease" do
    {:ok, lease_count} =
      Ecto.take_lease("app_name", "stream_name", "a.b.c", "new_owner", 1, repo: Repo)

    assert lease_count == 2
  end

  test "returns error when taking a shard_lease" do
    {:error, error} =
      Ecto.take_lease("app_name", "stream_name", "a.b.c", "test_owner", 1, repo: Repo)

    assert error == :lease_take_failed
  end

  test "updates shard_lease checkpoint" do
    assert Ecto.update_checkpoint("app_name", "stream_name", "a.b.c", "test_owner", "checkpoint_1",
             repo: Repo
           ) ==
             :ok
  end

  describe "update_checkpoint ownership guard" do
    test "guards the write on lease_owner but not lease_count" do
      params = %{
        shard_id: "a.b.c",
        app_name: "app_name",
        stream_name: "stream_name",
        lease_owner: "test_owner"
      }

      assert {:ok, _} = ShardLeases.update_checkpoint(params, "checkpoint_1", CaptureRepo)

      assert_received {:update_all, query}
      query_string = inspect(query)

      # The write must survive a concurrent lease renewal bumping lease_count,
      # so it may only condition on ownership, never on lease_count.
      assert query_string =~ "lease_owner"
      refute query_string =~ "lease_count"
    end

    test "maps a lost lease (no matching row) to :update_checkpoint_failed" do
      assert {:error, :update_checkpoint_failed} =
               Ecto.update_checkpoint(
                 "app_name",
                 "stream_name",
                 "a.b.c",
                 "test_owner",
                 "checkpoint_1",
                 repo: NoRowRepo
               )
    end
  end

  test "closes shard" do
    assert Ecto.close_shard("app_name", "stream_name", "a.b.c", "test_owner", repo: Repo) == :ok
  end
end
