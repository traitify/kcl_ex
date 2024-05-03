defmodule KinesisClient.Stream.AppState.EctoTest do
  use ExUnit.Case

  alias KinesisClient.Ecto.Repo
  alias KinesisClient.Stream.AppState.Ecto

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

  test "closes shard" do
    assert Ecto.close_shard("app_name", "stream_name", "a.b.c", "test_owner", repo: Repo) == :ok
  end

  describe "delete_all_leases_and_restart_workers" do
    setup do
      Process.flag(:trap_exit, true)

      TestSupervisor.start_link([])
      {:ok, supervisor: TestSupervisor}
    end

    test "deletes all leases and restarts the supervisor if the supervisor is running", %{supervisor: supervisor} do
      result = Ecto.delete_all_leases_and_restart_workers(supervisor, "app_name", repo: Repo)
      assert result == {:ok, "Shard leases deleted and workers restarted"}
    end

    test "returns an error if the supervisor is not found", _context do
      result = Ecto.delete_all_leases_and_restart_workers(:non_existent_supervisor, "app_name", repo: Repo)
      assert result == {:error, "Supervisor not running"}
    end
  end
end
