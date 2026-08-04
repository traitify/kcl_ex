defmodule KinesisClient.Stream.Shard.LeaseV2Test do
  use KinesisClient.Case

  alias KinesisClient.Stream.AppState.ShardLease
  alias KinesisClient.Stream.Shard.LeaseV2

  defmodule NotifyingPipeline do
    @moduledoc false
    def start(state) do
      send(state.notify, {:pipeline_started, state.shard_id})
      :ok
    end

    def stop(state) do
      send(state.notify, {:pipeline_stopped, state.shard_id})
      :ok
    end
  end

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

  test "tracks an existing lease owned by another worker without taking it" do
    shard_lease_count = 12
    lease_opts = build_lease_opts()
    shard_lease = build_shard_lease(lease_count: shard_lease_count, lease_owner: worker_ref())

    stub(AppStateMock, :get_lease, fn _in_app_name, _in_stream_name, _in_shard_id, _ ->
      shard_lease
    end)

    {:ok, pid} = start_supervised({LeaseV2, lease_opts})

    assert_receive {:initialized, lease_state}, 1_000
    assert lease_state.lease_holder == false
    assert lease_state.lease_count == shard_lease_count
    assert Process.alive?(pid)
    stop_supervised(LeaseV2)
  end

  test "stops the pipeline and releases lease_holder when renewal fails" do
    current_worker = worker_ref()

    lease_opts =
      build_lease_opts(
        lease_owner: current_worker,
        renew_interval: 200,
        pipeline: NotifyingPipeline
      )

    owned_shard_lease =
      build_shard_lease(
        lease_count: 1,
        lease_owner: current_worker,
        shard_id: lease_opts[:shard_id]
      )

    AppStateMock
    |> expect(:get_lease, fn _in_app_name, _in_stream_name, _in_shard_id, _ ->
      :not_found
    end)
    |> stub(:get_lease, fn _in_app_name, _in_stream_name, _in_shard_id, _ ->
      owned_shard_lease
    end)
    |> stub(:create_lease, fn _app_name, _stream_name, _shard_id, _lease_owner, _opts ->
      :ok
    end)
    |> stub(:renew_lease, fn _app_name, _stream_name, _shard_lease, _opts ->
      {:error, :lease_renew_failed}
    end)

    {:ok, pid} = start_supervised({LeaseV2, lease_opts})

    assert_receive {:initialized, lease_state}, 1_000
    assert lease_state.lease_holder == true
    assert_receive {:pipeline_started, _shard_id}, 1_000

    assert_receive {:lease_renew_failed, lease_state}, 1_000
    assert lease_state.lease_holder == false
    assert_receive {:pipeline_stopped, _shard_id}, 1_000
    assert Process.alive?(pid)
    stop_supervised(LeaseV2)
  end

  test "reclaims a lease the AppState says it owns but it is not holding" do
    current_worker = worker_ref()

    lease_opts =
      build_lease_opts(
        lease_owner: current_worker,
        renew_interval: 200,
        pipeline: NotifyingPipeline
      )

    # e.g. a renewal that "failed" after actually being applied (lost
    # response + AWS retry fails the conditional check), or a restart of this
    # process after taking the lease: the row names this worker as owner but
    # lease_holder starts out (or was reset to) false. Without reclaiming,
    # renewing requires lease_holder and taking rejects the current owner, so
    # the shard would never be consumed again.
    owned_shard_lease =
      build_shard_lease(
        lease_count: 1,
        lease_owner: current_worker,
        shard_id: lease_opts[:shard_id]
      )

    AppStateMock
    |> stub(:get_lease, fn _in_app_name, _in_stream_name, _in_shard_id, _ ->
      owned_shard_lease
    end)
    |> stub(:renew_lease, fn _app_name, _stream_name, shard_lease, _opts ->
      {:ok, shard_lease.lease_count + 1}
    end)

    {:ok, pid} = start_supervised({LeaseV2, lease_opts})

    assert_receive {:initialized, lease_state}, 1_000
    assert lease_state.lease_holder == false

    assert_receive {:lease_reclaimed, lease_state}, 1_000
    assert lease_state.lease_holder == true
    assert lease_state.lease_count == owned_shard_lease.lease_count + 1
    assert_receive {:pipeline_started, _shard_id}, 1_000
    assert Process.alive?(pid)
    stop_supervised(LeaseV2)
  end

  describe ":steal_lease message" do
    test "steals the lease from its current owner and starts the pipeline" do
      current_worker = worker_ref()
      victim = worker_ref()

      lease_opts =
        build_lease_opts(lease_owner: current_worker, pipeline: NotifyingPipeline)

      shard_lease =
        build_shard_lease(
          lease_count: 8,
          lease_owner: victim,
          shard_id: lease_opts[:shard_id]
        )

      stub(AppStateMock, :get_lease, fn _in_app_name, _in_stream_name, _in_shard_id, _ ->
        shard_lease
      end)
      |> stub(:take_lease, fn app_name, stream_name, shard_id, new_owner, lc, _opts ->
        assert app_name == lease_opts[:app_name]
        assert stream_name == lease_opts[:stream_name]
        assert shard_id == lease_opts[:shard_id]
        assert new_owner == current_worker
        assert lc == shard_lease.lease_count

        {:ok, lc + 1}
      end)

      {:ok, pid} = start_supervised({LeaseV2, lease_opts})

      assert_receive {:initialized, lease_state}, 1_000
      assert lease_state.lease_holder == false

      send(pid, {:steal_lease, victim})

      assert_receive {:lease_stolen, lease_state}, 1_000
      assert lease_state.lease_holder == true
      assert lease_state.lease_count == shard_lease.lease_count + 1
      assert lease_state.lease_owner == current_worker
      assert_receive {:pipeline_started, _shard_id}, 1_000
      assert Process.alive?(pid)
      stop_supervised(LeaseV2)
    end

    test "steals with the freshly read lease_count when the state count is stale" do
      current_worker = worker_ref()
      other_worker = worker_ref()
      lease_opts = build_lease_opts(lease_owner: current_worker, pipeline: NotifyingPipeline)

      # The count read at init (synced into state) is 8, but by the time the
      # steal request arrives the owner has renewed the lease to 9. The steal
      # must use the fresh count or the optimistic lock will always fail.
      stale_shard_lease =
        build_shard_lease(
          lease_count: 8,
          lease_owner: other_worker,
          shard_id: lease_opts[:shard_id]
        )

      fresh_shard_lease =
        build_shard_lease(
          lease_count: 9,
          lease_owner: other_worker,
          shard_id: lease_opts[:shard_id]
        )

      AppStateMock
      |> expect(:get_lease, fn _in_app_name, _in_stream_name, _in_shard_id, _ ->
        stale_shard_lease
      end)
      |> stub(:get_lease, fn _in_app_name, _in_stream_name, _in_shard_id, _ ->
        fresh_shard_lease
      end)
      |> stub(:take_lease, fn _app_name, _stream_name, _shard_id, new_owner, lc, _opts ->
        assert new_owner == current_worker
        assert lc == fresh_shard_lease.lease_count

        {:ok, lc + 1}
      end)

      {:ok, pid} = start_supervised({LeaseV2, lease_opts})

      assert_receive {:initialized, lease_state}, 1_000
      assert lease_state.lease_count == stale_shard_lease.lease_count

      send(pid, {:steal_lease, other_worker})

      assert_receive {:lease_stolen, lease_state}, 1_000
      assert lease_state.lease_holder == true
      assert lease_state.lease_count == fresh_shard_lease.lease_count + 1
      assert Process.alive?(pid)
      stop_supervised(LeaseV2)
    end

    test "skips the steal when the lease changed owners since the decision" do
      lease_opts = build_lease_opts()
      chosen_victim = worker_ref()
      new_owner = worker_ref()

      # The rebalancer picked chosen_victim, but by the time the request
      # arrives the lease belongs to someone else — stealing now would hit a
      # worker the balancing decision never targeted.
      shard_lease =
        build_shard_lease(lease_owner: new_owner, shard_id: lease_opts[:shard_id])

      stub(AppStateMock, :get_lease, fn _in_app_name, _in_stream_name, _in_shard_id, _ ->
        shard_lease
      end)

      {:ok, pid} = start_supervised({LeaseV2, lease_opts})

      assert_receive {:initialized, %{lease_holder: false}}, 1_000

      send(pid, {:steal_lease, chosen_victim})

      refute_receive {:lease_stolen, _}, 200
      assert Process.alive?(pid)
      stop_supervised(LeaseV2)
    end

    test "ignores the request when already the lease holder" do
      lease_opts = build_lease_opts(pipeline: KinesisClient.TestPipeline)

      AppStateMock
      |> stub(:get_lease, fn _in_app_name, _in_stream_name, _in_shard_id, _ ->
        :not_found
      end)
      |> stub(:create_lease, fn _app_name, _stream_name, _shard_id, _lease_owner, _opts ->
        :ok
      end)

      {:ok, pid} = start_supervised({LeaseV2, lease_opts})

      assert_receive {:initialized, %{lease_holder: true}}, 1_000

      send(pid, {:steal_lease, worker_ref()})

      refute_receive {:lease_stolen, _}, 200
      assert Process.alive?(pid)
      stop_supervised(LeaseV2)
    end

    test "ignores the request when the shard is completed" do
      lease_opts = build_lease_opts()

      shard_lease =
        build_shard_lease(
          lease_owner: worker_ref(),
          shard_id: lease_opts[:shard_id],
          completed: true
        )

      stub(AppStateMock, :get_lease, fn _in_app_name, _in_stream_name, _in_shard_id, _ ->
        shard_lease
      end)

      {:ok, pid} = start_supervised({LeaseV2, lease_opts})

      assert_receive {:initialized, %{lease_holder: false}}, 1_000

      send(pid, {:steal_lease, shard_lease.lease_owner})

      refute_receive {:lease_stolen, _}, 200
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

  test "doesn't take an expired lease when the shard is completed" do
    lease_opts = build_lease_opts(lease_expiry: 500, renew_interval: 1_000)
    shard_lease = build_shard_lease(lease_count: 12, completed: true)

    # take_lease is intentionally left unstubbed: a completed shard must never
    # be taken, so any call would raise and fail this test.
    stub(AppStateMock, :get_lease, fn _in_app_name, _in_stream_name, _in_shard_id, _ ->
      shard_lease
    end)

    {:ok, pid} = start_supervised({LeaseV2, lease_opts})

    assert_receive {:initialized, %{lease_holder: false}}, 1_000
    refute_receive {:lease_taken, _}, 1_500
    assert_receive {:tracking_lease, lease_state}, 5_000
    assert lease_state.lease_holder == false
    assert lease_state.lease_count == shard_lease.lease_count
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
