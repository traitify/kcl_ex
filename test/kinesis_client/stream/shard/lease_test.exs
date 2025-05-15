defmodule KinesisClient.Stream.Shard.LeaseTest do
  use KinesisClient.Case

  alias KinesisClient.Stream.AppState.ShardLease
  alias KinesisClient.Stream.Shard.Lease

  test "creates and takes AppState.ShardLease if none already exists" do
    lease_opts = build_lease_opts(pipeline: KinesisClient.TestPipeline)

    AppStateMock
    |> stub(:get_lease, fn in_app_name, in_stream_name, in_shard_id, _ ->
      assert in_app_name == lease_opts[:app_name]
      assert in_stream_name == lease_opts[:stream_name]
      assert in_shard_id == lease_opts[:shard_id]
      :not_found
    end)
    |> stub(:create_lease, fn app_name, stream_name, shard_id, lease_owner, _opts ->
      assert app_name == lease_opts[:app_name]
      assert stream_name == lease_opts[:stream_name]
      assert shard_id == lease_opts[:shard_id]
      assert lease_owner == lease_opts[:lease_owner]

      :ok
    end)

    {:ok, pid} = start_supervised({Lease, lease_opts})

    assert_receive {:initialized, lease_state}, 1_000

    inspect(lease_state, label: "lease_state")
    assert lease_state.lease_holder == true
    assert lease_state.lease_count == 1

    assert Process.alive?(pid)
  end

  test "when another Shard created the ShardLease first then set lease_holder: false" do
    lease_opts = build_lease_opts()

    AppStateMock
    |> stub(:get_lease, fn _in_app_name, _in_stream_name, _in_shard_id, _ ->
      :not_found
    end)
    |> expect(:create_lease, fn _app_name, _in_stream_name, _shard_id, _lease_owner, _opts ->
      :already_exists
    end)

    {:ok, pid} = start_supervised({Lease, lease_opts})

    assert_receive {:initialized, lease_state}, 1_000

    assert lease_state.lease_holder == false
    assert lease_state.lease_count == nil

    assert Process.alive?(pid)
  end

  describe "when shard_lease already exists" do
    test "and when lease_holder does not match then set lease_holder and lease_count" do
      shard_lease_count = 12
      lease_opts = build_lease_opts()
      shard_lease = build_shard_lease(lease_count: shard_lease_count)

      stub(AppStateMock, :get_lease, fn _in_app_name, _in_stream_name, _in_shard_id, _ ->
        shard_lease
      end)
      |> stub(:get_leases_by_worker, fn _in_app_name, _in_stream_name, _lease_owner, _ ->
        [shard_lease]
      end)

      {:ok, pid} = start_supervised({Lease, lease_opts})

      assert_receive {:initialized, lease_state}, 1_000

      assert lease_state.lease_holder == false
      assert lease_state.lease_count == shard_lease_count

      assert Process.alive?(pid)
    end

    test "and when lease_owner matches shard lease owner then renew_lease" do
      shard_lease_count = 12
      lease_opts = build_lease_opts(lease_holder: true)

      shard_lease =
        build_shard_lease(lease_count: shard_lease_count, lease_owner: lease_opts[:lease_owner])

      AppStateMock
      |> stub(:get_lease, fn _in_app_name, _in_stream_name, _in_shard_id, _ ->
        shard_lease
      end)
      |> expect(:renew_lease, fn app_name, stream_name, %{lease_count: lc} = sl, _ ->
        assert app_name == lease_opts[:app_name]
        assert stream_name == lease_opts[:stream_name]
        assert sl.shard_id == shard_lease.shard_id
        assert sl.lease_count == shard_lease.lease_count
        assert sl.lease_owner == shard_lease.lease_owner
        {:ok, lc + 1}
      end)

      {:ok, pid} = start_supervised({Lease, lease_opts})

      assert_receive {:lease_renewed, lease_state}, 1_000

      assert lease_state.lease_holder == true
      assert lease_state.lease_count == shard_lease_count + 1

      assert Process.alive?(pid)
    end
  end

  test "takes lease if lease_expiry exceeded" do
    shard_lease_count = 12
    lease_opts = build_lease_opts(lease_expiry: 500, renew_interval: 1_000)
    shard_lease = build_shard_lease(lease_count: shard_lease_count)

    AppStateMock
    |> stub(:get_lease, fn _in_app_name, _in_stream_name, _in_shard_id, _ ->
      shard_lease
    end)
    |> stub(:get_leases_by_worker, fn _in_app_name, _in_stream_name, _lease_owner, _ ->
      [shard_lease]
    end)
    |> expect(:take_lease, fn app_name, stream_name, shard_id, new_owner, lc, _opts ->
      assert app_name == lease_opts[:app_name]
      assert stream_name == lease_opts[:stream_name]
      assert shard_id == lease_opts[:shard_id]
      assert new_owner == lease_opts[:lease_owner]
      assert lc == 12
      {:ok, lc + 1}
    end)

    {:ok, _pid} = start_supervised({Lease, lease_opts})

    assert_receive {:initialized, %{lease_count_increment_time: lcit} = lease_state}, 1_000
    assert lease_state.lease_holder == false
    assert lease_state.lease_count == shard_lease.lease_count

    assert_receive {:tracking_lease, _}, 5_000
    assert_receive {:lease_taken, lease_state}, 15_000

    stop_supervised(Lease)
    assert lease_state.lease_holder == true
    assert lease_state.lease_count == shard_lease.lease_count + 1
    assert lcit < lease_state.lease_count_increment_time
  end

  test "doesn't take lease if lease_expiry not exceeded" do
    shard_lease_count = 12
    lease_opts = build_lease_opts(lease_expiry: 5_000, renew_interval: 600)
    shard_lease = build_shard_lease(lease_count: shard_lease_count)

    stub(AppStateMock, :get_lease, fn _in_app_name, _in_stream_name, _in_shard_id, _ ->
      shard_lease
    end)
    |> stub(:get_leases_by_worker, fn _in_app_name, _in_stream_name, _lease_owner, _ ->
      [shard_lease]
    end)

    {:ok, _pid} = start_supervised({Lease, lease_opts})

    assert_receive {:initialized, %{lease_count_increment_time: _lcit} = lease_state}, 1_000
    assert lease_state.lease_holder == false

    assert_receive {:tracking_lease, lease_state}, 5_000
    assert lease_state.lease_holder == false
    assert lease_state.lease_count == shard_lease.lease_count

    assert_receive {:tracking_lease, lease_state}, 5_000
    stop_supervised(Lease)
    assert lease_state.lease_holder == false
    assert lease_state.lease_count == shard_lease.lease_count
  end

  describe "the limit of renewing lease" do
    test "when the limit is set and met" do
      shard_lease_count = 12

      lease_opts =
        build_lease_opts(lease_renewal_limit: 2, lease_renewal_count: 2, lease_holder: true)

      shard_lease =
        build_shard_lease(lease_count: shard_lease_count, lease_owner: lease_opts[:lease_owner])

      AppStateMock
      |> stub(:get_lease, fn _in_app_name, _in_stream_name, _in_shard_id, _ ->
        shard_lease
      end)

      {:ok, lease} = start_supervised({Lease, lease_opts})

      assert_receive {:lease_released, lease_state}, 1_000

      assert lease_state.lease_holder == false
      assert lease_state.lease_renewal_count == 0

      assert Process.alive?(lease)
    end

    test "when there is no limit" do
      shard_lease_count = 12

      lease_opts =
        build_lease_opts(lease_renewal_limit: -1, lease_renewal_count: 10, lease_holder: true)

      shard_lease =
        build_shard_lease(lease_count: shard_lease_count, lease_owner: lease_opts[:lease_owner])

      AppStateMock
      |> stub(:get_lease, fn _in_app_name, _in_stream_name, _in_shard_id, _ ->
        shard_lease
      end)
      |> expect(:renew_lease, fn app_name, stream_name, %{lease_count: lc} = sl, _ ->
        assert app_name == lease_opts[:app_name]
        assert stream_name == lease_opts[:stream_name]
        assert sl.shard_id == shard_lease.shard_id
        assert sl.lease_count == shard_lease.lease_count
        assert sl.lease_owner == shard_lease.lease_owner
        {:ok, lc + 1}
      end)

      {:ok, lease} = start_supervised({Lease, lease_opts})

      assert_receive {:lease_renewed, lease_state}, 1_000

      assert lease_state.lease_holder == true
      assert lease_state.lease_count == shard_lease_count + 1
      assert lease_state.lease_renewal_count == 11

      assert Process.alive?(lease)
    end
  end

  defp build_lease_opts(overrides \\ []) do
    Keyword.merge(
      [
        coordinator_name: MyStreamCoordinator,
        shard_id: "shard-000001",
        lease_owner: worker_ref(),
        app_name: "my_streaming_app",
        stream_name: "my_stream",
        notify: self(),
        app_state_opts: [adapter: :test]
      ],
      overrides
    )
  end

  def build_shard_lease(overrides \\ []) do
    default = [
      shard_id: "shard-000001",
      checkpoint: :rand.uniform(32),
      lease_owner: worker_ref(),
      lease_count: 1,
      completed: false
    ]

    merged = Keyword.merge(default, overrides)

    struct(ShardLease, merged)
  end
end
