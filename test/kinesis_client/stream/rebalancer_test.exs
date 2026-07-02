defmodule KinesisClient.Stream.RebalancerTest do
  use KinesisClient.Case

  import KinesisClient.Util

  alias KinesisClient.Stream.AppState.ShardLease
  alias KinesisClient.Stream.Rebalancer
  alias KinesisClient.Stream.Shard.LeaseV2

  test "notifies :all_balanced and requests no steals when the load is balanced" do
    opts = build_rebalancer_opts()
    lease_owner = opts[:lease_owner]
    other_worker = worker_ref()

    stub(AppStateMock, :all_incomplete_leases, fn _app_name, _stream_name, _opts ->
      [
        build_shard_lease(shard_id: "shard-000001", lease_owner: lease_owner),
        build_shard_lease(shard_id: "shard-000002", lease_owner: lease_owner),
        build_shard_lease(shard_id: "shard-000003", lease_owner: other_worker),
        build_shard_lease(shard_id: "shard-000004", lease_owner: other_worker)
      ]
    end)

    {:ok, pid} = start_supervised({Rebalancer, opts})

    assert_receive {:all_balanced, _state}, 1_000
    assert Process.alive?(pid)
    stop_supervised(Rebalancer)
  end

  test "requests a steal from the local lease process of an overloaded worker's shard" do
    opts = build_rebalancer_opts()
    victim = worker_ref()

    victim_leases = [
      build_shard_lease(shard_id: "shard-000001", lease_owner: victim),
      build_shard_lease(shard_id: "shard-000002", lease_owner: victim),
      build_shard_lease(shard_id: "shard-000003", lease_owner: victim),
      build_shard_lease(shard_id: "shard-000004", lease_owner: victim)
    ]

    # Only shard-000002 has a local lease process, so despite the random
    # candidate order the steal request can only go there.
    register_lease_process(opts, "shard-000002")

    stub(AppStateMock, :all_incomplete_leases, fn _app_name, _stream_name, _opts ->
      victim_leases
    end)

    {:ok, pid} = start_supervised({Rebalancer, opts})

    assert_receive {:steal_requested, "shard-000002"}, 1_000
    assert_receive {:lease_message, "shard-000002", {:steal_lease, ^victim}}, 1_000
    assert Process.alive?(pid)
    stop_supervised(Rebalancer)
  end

  test "requests at most max_leases_to_steal steals per tick" do
    # Interval sized so the first tick (jittered to 450-750ms) lands inside
    # the assert window, while the second tick can't land inside the refute
    # window below — the next tick would legitimately steal again.
    opts = build_rebalancer_opts(rebalance_interval: 600)
    victim = worker_ref()

    # 4 victim leases and none of ours: a deficit of 2, but the default
    # max_leases_to_steal of 1 caps the tick at a single steal request.
    victim_leases = [
      build_shard_lease(shard_id: "shard-000001", lease_owner: victim),
      build_shard_lease(shard_id: "shard-000002", lease_owner: victim),
      build_shard_lease(shard_id: "shard-000003", lease_owner: victim),
      build_shard_lease(shard_id: "shard-000004", lease_owner: victim)
    ]

    Enum.each(victim_leases, &register_lease_process(opts, &1.shard_id))

    stub(AppStateMock, :all_incomplete_leases, fn _app_name, _stream_name, _opts ->
      victim_leases
    end)

    {:ok, pid} = start_supervised({Rebalancer, opts})

    assert_receive {:lease_message, _shard_id, {:steal_lease, _victim}}, 1_000
    refute_receive {:lease_message, _shard_id, {:steal_lease, _victim}}, 200
    assert Process.alive?(pid)
    stop_supervised(Rebalancer)
  end

  test "clamps steals to the deficit even when max_leases_to_steal is higher" do
    # Counts of {victim: 4, me: 0} mean a target of 2 and a deficit of 2:
    # stealing max_leases_to_steal (3) would overshoot to {1, 3} and the
    # imbalance would flip back and forth forever.
    opts = build_rebalancer_opts(rebalance_interval: 600, max_leases_to_steal: 3)
    victim = worker_ref()

    victim_leases = [
      build_shard_lease(shard_id: "shard-000001", lease_owner: victim),
      build_shard_lease(shard_id: "shard-000002", lease_owner: victim),
      build_shard_lease(shard_id: "shard-000003", lease_owner: victim),
      build_shard_lease(shard_id: "shard-000004", lease_owner: victim)
    ]

    Enum.each(victim_leases, &register_lease_process(opts, &1.shard_id))

    stub(AppStateMock, :all_incomplete_leases, fn _app_name, _stream_name, _opts ->
      victim_leases
    end)

    {:ok, pid} = start_supervised({Rebalancer, opts})

    assert_receive {:lease_message, _shard_id, {:steal_lease, _victim}}, 1_000
    assert_receive {:lease_message, _shard_id, {:steal_lease, _victim}}, 1_000
    refute_receive {:lease_message, _shard_id, {:steal_lease, _victim}}, 200
    assert Process.alive?(pid)
    stop_supervised(Rebalancer)
  end

  test "survives a failing balancing query and keeps ticking" do
    opts = build_rebalancer_opts()

    stub(AppStateMock, :all_incomplete_leases, fn _app_name, _stream_name, _opts ->
      raise "throttled scan"
    end)

    {:ok, pid} = start_supervised({Rebalancer, opts})

    assert_receive {:rebalance_failed, _error}, 1_000
    assert_receive {:rebalance_failed, _error}, 1_000
    assert Process.alive?(pid)
    stop_supervised(Rebalancer)
  end

  # Registers a stand-in for the shard's LeaseV2 process that forwards any
  # message it receives back to the test process, tagged with the shard_id.
  defp register_lease_process(opts, shard_id) do
    test_pid = self()

    pid =
      spawn_link(fn ->
        receive do
          msg -> send(test_pid, {:lease_message, shard_id, msg})
        end
      end)

    name = register_name(LeaseV2, opts[:app_name], opts[:stream_name], [shard_id])
    Process.register(pid, name)
  end

  defp build_shard_lease(overrides) do
    default = [
      shard_id: "shard-000001",
      lease_owner: worker_ref(),
      lease_count: 1,
      completed: false
    ]

    struct(ShardLease, Keyword.merge(default, overrides))
  end

  defp build_rebalancer_opts(overrides \\ []) do
    Keyword.merge(
      [
        app_name: "my_streaming_app",
        stream_name: "stream-#{:rand.uniform(100_000)}",
        lease_owner: worker_ref(),
        app_state_opts: [adapter: :test],
        rebalance_interval: 200,
        notify: self()
      ],
      overrides
    )
  end
end
