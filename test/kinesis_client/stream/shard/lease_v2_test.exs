defmodule KinesisClient.Stream.Shard.LeaseV2Test do
  use KinesisClient.Case

  alias KinesisClient.Stream.AppState.ShardLease
  alias KinesisClient.Stream.Shard.LeaseV2

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

    {:ok, pid} = start_supervised({LeaseV2, lease_opts})

    assert_receive {:initialized, lease_state}, 1_000
    assert lease_state.lease_holder == true
    assert lease_state.lease_count == 1
    assert Process.alive?(pid)
    stop_supervised(LeaseV2)
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

    {:ok, pid} = start_supervised({LeaseV2, lease_opts})

    assert_receive {:initialized, lease_state}, 1_000
    assert lease_state.lease_holder == false
    assert lease_state.lease_count == nil
    assert Process.alive?(pid)
    stop_supervised(LeaseV2)
  end

  describe "when shard_lease already exists" do
    test "and lease owner does not currently have any leases and it's all balanced so move on" do
      shard_lease_count = 12
      lease_opts = build_lease_opts()
      other_worker = lease_opts[:lease_owner]
      shard_lease = build_shard_lease(lease_count: shard_lease_count, lease_owner: other_worker)
      current_worker = worker_ref()

      stub(AppStateMock, :get_lease, fn _in_app_name, _in_stream_name, _in_shard_id, _ ->
        shard_lease
      end)
      |> stub(:get_leases_by_worker, fn _in_app_name, _in_stream_name, _lease_owner, _ ->
        []
      end)
      |> stub(:total_incomplete_lease_counts_by_worker, fn _app_name, _stream_name, _opts ->
        [{other_worker, 1}]
      end)
      |> stub(:all_incomplete_leases, fn _app_name, _stream_name, _opts ->
        [shard_lease]
      end)
      |> stub(:lease_owner_with_most_leases, fn _app_name, _stream_name, _opts ->
        [shard_lease]
      end)
      |> stub(:take_lease, fn app_name, stream_name, shard_id, new_owner, lc, _opts ->
        assert app_name == lease_opts[:app_name]
        assert stream_name == lease_opts[:stream_name]
        assert shard_id == lease_opts[:shard_id]
        assert new_owner == current_worker
        assert lc == 12

        {:ok, lc + 1}
      end)

      {:ok, pid} =
        start_supervised(
          {LeaseV2, build_lease_opts(shard_id: shard_lease.shard_id, lease_owner: current_worker)}
        )

      assert_receive {:all_balanced, lease_state}, 1_000
      assert lease_state.lease_holder == false
      assert lease_state.lease_count == shard_lease_count
      assert Process.alive?(pid)
      stop_supervised(LeaseV2)
    end

    test "and all is balanced so move on" do
      shard_lease_count = 12
      lease_opts = build_lease_opts()
      current_worker = lease_opts[:lease_owner]
      shard_lease_1 = build_shard_lease(lease_count: shard_lease_count, lease_owner: current_worker)
      other_worker = worker_ref()
      shard_lease_2 = build_shard_lease(lease_count: 5, lease_owner: other_worker)

      stub(AppStateMock, :get_lease, fn _in_app_name, _in_stream_name, _in_shard_id, _ ->
        shard_lease_1
      end)
      |> stub(:get_leases_by_worker, fn _in_app_name, _in_stream_name, _lease_owner, _ ->
        [shard_lease_1]
      end)
      |> stub(:total_incomplete_lease_counts_by_worker, fn _app_name, _stream_name, _opts ->
        [{current_worker, 1}, {worker_ref(), 1}]
      end)
      |> stub(:all_incomplete_leases, fn _app_name, _stream_name, _opts ->
        [shard_lease_1, shard_lease_2]
      end)

      {:ok, pid} = start_supervised({LeaseV2, lease_opts})

      assert_receive {:all_balanced, lease_state}, 1_000
      assert lease_state.shard_id == shard_lease_1.shard_id
      assert lease_state.lease_count == shard_lease_count
      assert lease_state.lease_owner == current_worker
      assert Process.alive?(pid)
      stop_supervised(LeaseV2)
    end

    test "and it's not balanced so steal from overloaded worker" do
      lease_opts = build_lease_opts()
      worker_1 = worker_ref()

      shard_lease_1 =
        build_shard_lease(lease_count: 12, lease_owner: worker_1, shard_id: "shard-000001")

      worker_2 = worker_ref()

      shard_lease_2 =
        build_shard_lease(lease_count: 10, lease_owner: worker_2, shard_id: "shard-000002")

      shard_lease_3 =
        build_shard_lease(lease_count: 10, lease_owner: worker_2, shard_id: "shard-000003")

      shard_lease_4 =
        build_shard_lease(lease_count: 10, lease_owner: worker_2, shard_id: "shard-000004")

      shard_lease_5 =
        build_shard_lease(lease_count: 10, lease_owner: worker_2, shard_id: "shard-000005")

      worker_3 = worker_ref()

      shard_lease_6 =
        build_shard_lease(lease_count: 8, lease_owner: worker_3, shard_id: "shard-000006")

      shard_lease_7 =
        build_shard_lease(lease_count: 8, lease_owner: worker_3, shard_id: "shard-000007")

      shard_lease_8 =
        build_shard_lease(lease_count: 8, lease_owner: worker_3, shard_id: "shard-000008")

      shard_lease_9 =
        build_shard_lease(lease_count: 8, lease_owner: worker_3, shard_id: "shard-000009")

      stub(AppStateMock, :get_lease, fn _in_app_name, _in_stream_name, _in_shard_id, _ ->
        shard_lease_6
      end)
      |> stub(:get_leases_by_worker, fn _in_app_name, _in_stream_name, _lease_owner, _ ->
        [shard_lease_1]
      end)
      |> stub(:total_incomplete_lease_counts_by_worker, fn _app_name, _stream_name, _opts ->
        [{worker_1, 1}, {worker_2, 4}, {worker_3, 4}]
      end)
      |> stub(:all_incomplete_leases, fn _app_name, _stream_name, _opts ->
        [
          shard_lease_1,
          shard_lease_2,
          shard_lease_3,
          shard_lease_4,
          shard_lease_5,
          shard_lease_6,
          shard_lease_7,
          shard_lease_8,
          shard_lease_9
        ]
      end)
      |> stub(:lease_owner_with_most_leases, fn _app_name, _stream_name, _opts ->
        [shard_lease_6, shard_lease_7, shard_lease_8, shard_lease_9]
      end)
      |> stub(:take_lease, fn app_name, stream_name, shard_id, new_owner, lc, _opts ->
        assert app_name == lease_opts[:app_name]
        assert stream_name == lease_opts[:stream_name]

        assert shard_id in [
                 shard_lease_6.shard_id,
                 shard_lease_7.shard_id,
                 shard_lease_8.shard_id,
                 shard_lease_9.shard_id
               ]

        assert new_owner == worker_1
        assert lc == 8

        {:ok, lc + 1}
      end)

      {:ok, pid} =
        start_supervised(
          {LeaseV2, build_lease_opts(shard_id: shard_lease_6.shard_id, lease_owner: worker_1)}
        )

      assert_receive {:lease_stolen, lease_state}, 1_000
      assert lease_state.lease_holder == true
      assert lease_state.lease_count == 9
      assert lease_state.lease_owner == worker_1
      assert lease_state.shard_id == shard_lease_6.shard_id
      assert Process.alive?(pid)
      stop_supervised(LeaseV2)
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
    |> stub(:total_incomplete_lease_counts_by_worker, fn _app_name, _stream_name, _opts ->
      [{lease_opts[:application], 1}]
    end)
    |> stub(:all_incomplete_leases, fn _app_name, _stream_name, _opts ->
      [shard_lease]
    end)
    |> stub(:take_lease, fn app_name, stream_name, shard_id, new_owner, lc, _opts ->
      assert app_name == lease_opts[:app_name]
      assert stream_name == lease_opts[:stream_name]
      assert shard_id == lease_opts[:shard_id]
      assert new_owner == lease_opts[:lease_owner]
      assert lc == 12
      {:ok, lc + 1}
    end)

    {:ok, pid} = start_supervised({LeaseV2, lease_opts})

    assert_receive {:initialized, %{lease_count_increment_time: lcit} = lease_state}, 1_000
    assert lease_state.lease_holder == false
    assert lease_state.lease_count == shard_lease.lease_count
    assert_receive {:lease_taken, lease_state}, 15_000
    assert lease_state.lease_holder == true
    assert lease_state.lease_count == shard_lease.lease_count + 1
    assert lcit < lease_state.lease_count_increment_time
    assert Process.alive?(pid)
    stop_supervised(LeaseV2)
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
    |> stub(:total_incomplete_lease_counts_by_worker, fn _app_name, _stream_name, _opts ->
      [{lease_opts[:application], 1}]
    end)
    |> stub(:all_incomplete_leases, fn _app_name, _stream_name, _opts ->
      [shard_lease]
    end)

    {:ok, _pid} = start_supervised({LeaseV2, lease_opts})

    assert_receive {:initialized, %{lease_count_increment_time: _lcit} = lease_state}, 1_000
    assert lease_state.lease_holder == false
    assert_receive {:tracking_lease, lease_state}, 5_000
    assert lease_state.lease_holder == false
    assert lease_state.lease_count == shard_lease.lease_count
    assert_receive {:tracking_lease, lease_state}, 5_000
    assert lease_state.lease_holder == false
    assert lease_state.lease_count == shard_lease.lease_count
    stop_supervised(LeaseV2)
  end

  test "run load balancing" do
    shard_lease_count = 12
    lease_opts = build_lease_opts(rebalance_interval: 500)
    shard_lease = build_shard_lease(lease_count: shard_lease_count)

    stub(AppStateMock, :get_lease, fn _in_app_name, _in_stream_name, _in_shard_id, _ ->
      shard_lease
    end)
    |> stub(:get_leases_by_worker, fn _in_app_name, _in_stream_name, _lease_owner, _ ->
      [shard_lease]
    end)
    |> stub(:total_incomplete_lease_counts_by_worker, fn _app_name, _stream_name, _opts ->
      [{lease_opts[:application], 1}]
    end)
    |> stub(:all_incomplete_leases, fn _app_name, _stream_name, _opts ->
      [shard_lease]
    end)

    {:ok, pid} = start_supervised({LeaseV2, lease_opts})

    assert_receive {:initialized, %{lease_count_increment_time: _lcit} = lease_state}, 1_000
    assert lease_state.lease_holder == false
    assert_receive {:all_balanced, lease_state}, 1_000
    assert lease_state.lease_count == shard_lease.lease_count
    Process.alive?(pid)
    stop_supervised(LeaseV2)
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
