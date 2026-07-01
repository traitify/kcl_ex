defmodule KinesisClient.Stream.RebalancerTest do
  use KinesisClient.Case

  import KinesisClient.Util

  alias KinesisClient.Stream.AppState.ShardLease
  alias KinesisClient.Stream.Rebalancer
  alias KinesisClient.Stream.Shard.LeaseV2

  test "notifies :all_balanced and requests no steals when the load is balanced" do
    opts = build_rebalancer_opts()
    lease_owner = opts[:lease_owner]

    stub(AppStateMock, :total_incomplete_lease_counts_by_worker, fn _app_name,
                                                                    _stream_name,
                                                                    _opts ->
      [{lease_owner, 2}, {worker_ref(), 2}]
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

    AppStateMock
    |> stub(:total_incomplete_lease_counts_by_worker, fn _app_name, _stream_name, _opts ->
      [{victim, 4}]
    end)
    |> stub(:get_leases_by_worker, fn _app_name, _stream_name, in_lease_owner, _opts ->
      assert in_lease_owner == victim
      victim_leases
    end)

    {:ok, pid} = start_supervised({Rebalancer, opts})

    assert_receive {:steal_requested, "shard-000002"}, 1_000
    assert_receive {:lease_message, "shard-000002", :steal_lease}, 1_000
    assert Process.alive?(pid)
    stop_supervised(Rebalancer)
  end

  test "requests at most max_leases_to_steal steals per tick" do
    # Interval sized so the first tick (jittered to 450-750ms) lands inside
    # the assert window, while the second tick can't land inside the refute
    # window below — the next tick would legitimately steal again.
    opts = build_rebalancer_opts(rebalance_interval: 600)
    victim = worker_ref()

    victim_leases = [
      build_shard_lease(shard_id: "shard-000001", lease_owner: victim),
      build_shard_lease(shard_id: "shard-000002", lease_owner: victim)
    ]

    register_lease_process(opts, "shard-000001")
    register_lease_process(opts, "shard-000002")

    AppStateMock
    |> stub(:total_incomplete_lease_counts_by_worker, fn _app_name, _stream_name, _opts ->
      [{victim, 4}]
    end)
    |> stub(:get_leases_by_worker, fn _app_name, _stream_name, _lease_owner, _opts ->
      victim_leases
    end)

    {:ok, pid} = start_supervised({Rebalancer, opts})

    assert_receive {:lease_message, _shard_id, :steal_lease}, 1_000
    refute_receive {:lease_message, _shard_id, :steal_lease}, 200
    assert Process.alive?(pid)
    stop_supervised(Rebalancer)
  end

  test "skips completed shards when picking a steal candidate" do
    opts = build_rebalancer_opts()
    victim = worker_ref()

    victim_leases = [
      build_shard_lease(shard_id: "shard-000001", lease_owner: victim, completed: true),
      build_shard_lease(shard_id: "shard-000002", lease_owner: victim)
    ]

    register_lease_process(opts, "shard-000001")
    register_lease_process(opts, "shard-000002")

    AppStateMock
    |> stub(:total_incomplete_lease_counts_by_worker, fn _app_name, _stream_name, _opts ->
      [{victim, 4}]
    end)
    |> stub(:get_leases_by_worker, fn _app_name, _stream_name, _lease_owner, _opts ->
      victim_leases
    end)

    {:ok, pid} = start_supervised({Rebalancer, opts})

    assert_receive {:lease_message, "shard-000002", :steal_lease}, 1_000
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
