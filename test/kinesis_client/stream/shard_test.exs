defmodule KinesisClient.Stream.ShardTest do
  # Mox global mode: the processes under test (LeaseV2, Broadway producer) are
  # not the test process, so stubs must be visible from any process.
  use KinesisClient.Case, async: false

  alias KinesisClient.Stream.AppState.ShardLease
  alias KinesisClient.Stream.Shard

  @moduletag :capture_log

  # Regression for TD-6631: a producer that crashes while its worker still
  # holds the lease is restarted by Broadway with status: :stopped, and nothing
  # started it again while the lease kept being renewed — the shard sat
  # unconsumed until the lease moved to another worker (65h in production).
  # The lease process must now notice the stopped producer on its renew tick
  # and start it.
  test "restarts a crashed producer within one renew interval while the lease is held" do
    app_name = "shard-test-#{random_string()}"
    stream_name = "shard-test-stream"
    shard_id = "shardId-000000000001"
    lease_owner = worker_ref()
    renew_interval = 500
    test_pid = self()

    opts = [
      shard_name: Module.concat([Shard, app_name, stream_name, shard_id]),
      app_name: app_name,
      stream_name: stream_name,
      shard_id: shard_id,
      lease_owner: lease_owner,
      coordinator_name: Module.concat([ShardTestCoordinator, app_name]),
      app_state_opts: [adapter: :test],
      kinesis_opts: [adapter: KinesisMock],
      shard_consumer: KinesisClient.TestShardConsumer,
      lease_renew_interval: renew_interval,
      poll_interval: 60_000,
      processors: [default: [concurrency: 1, min_demand: 10, max_demand: 20]],
      batchers: [default: [concurrency: 1, batch_size: 40]]
    ]

    shard_lease = %ShardLease{
      shard_id: shard_id,
      lease_owner: lease_owner,
      lease_count: 1,
      checkpoint: nil,
      completed: false
    }

    # First lookup is the lease process finding no row and creating the lease;
    # every lookup after that (renewals, producer ownership checks) sees the
    # row owned by this worker, so the lease is renewed and never moves.
    AppStateMock
    |> expect(:get_lease, fn _app_name, _stream_name, _shard_id, _opts -> :not_found end)
    |> stub(:get_lease, fn _app_name, _stream_name, _shard_id, _opts -> shard_lease end)
    |> stub(:create_lease, fn _app_name, _stream_name, _shard_id, _lease_owner, _opts -> :ok end)
    |> stub(:renew_lease, fn _app_name, _stream_name, lease, _opts ->
      {:ok, lease.lease_count + 1}
    end)

    KinesisMock
    |> stub(:get_shard_iterator, fn _stream_name, _shard_id, _iterator_type, _opts ->
      {:ok, %{"ShardIterator" => "shard-iterator"}}
    end)
    |> stub(:get_records, fn _iterator, _opts ->
      send(test_pid, {:get_records, self()})
      {:ok, %{"NextShardIterator" => "next-iterator", "MillisBehindLatest" => 0, "Records" => []}}
    end)

    {:ok, _shard} = start_supervised({Shard, opts})

    # The lease is created and the producer fetches once.
    assert_receive {:get_records, producer}, 2_000

    # Simulate the production crash (a DBConnection error raised inside the
    # producer): Broadway restarts the producer, but with status: :stopped.
    ref = Process.monitor(producer)
    Process.exit(producer, :kill)
    assert_receive {:DOWN, ^ref, :process, ^producer, :killed}, 1_000

    # The restarted producer must resume fetching within one renew interval:
    # at most one interval until the next renewal tick, plus Broadway's restart
    # and the producer's start-up (milliseconds). A two-interval budget proves
    # the first tick after the crash did the restart.
    assert_receive {:get_records, restarted_producer}, 2 * renew_interval
    assert restarted_producer != producer
    assert Process.alive?(restarted_producer)
  end
end
