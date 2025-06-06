defmodule KinesisClient.Stream.CoordinatorTest do
  use KinesisClient.Case, async: false

  alias KinesisClient.Stream.AppState.ShardLease
  alias KinesisClient.Stream.Coordinator

  @stream_name "decline-roman-empire-test"
  @shard_count 6
  @supervisor_name MyShardSupervisor

  setup_all do
    KinesisClient.TestStream.create_stream(@stream_name, @shard_count)
    :ok
  end

  test "#remove_missing_parents" do
    %{"StreamDescription" => %{"Shards" => shards}} =
      KinesisClient.KinesisResponses.describe_stream()

    shards = Coordinator.remove_missing_parents(shards)
    first_shard = Enum.at(shards, 0)
    second_shard = Enum.at(shards, 1)
    assert first_shard["ParentShardId"] == nil
    assert second_shard["ParentShardId"] == first_shard["ShardId"]
  end

  test "describes kinesis stream and starts shards" do
    {:ok, _} =
      start_supervised({DynamicSupervisor, [strategy: :one_for_one, name: @supervisor_name]})

    {:ok, _consumer} = start_supervised({KinesisClient.TestConsumer, self()})

    opts = coordinator_opts()

    KinesisMock
    |> expect(:describe_stream, fn stream_name, _opts ->
      assert stream_name == @stream_name
      {:ok, KinesisClient.KinesisResponses.describe_stream()}
    end)
    |> stub(:get_records, fn _, _ ->
      []
    end)

    AppStateMock
    |> expect(:initialize, fn in_app_name, _ ->
      assert in_app_name == opts[:app_name]
      :ok
    end)
    |> stub(:get_lease, fn _, _, _, _ ->
      :not_found
    end)
    |> stub(:create_lease, fn _, _, _, _, _ ->
      :ok
    end)

    {:ok, _} = start_coordinator(opts)

    assert_receive {:shards, shards}, 5_000
    assert_receive {:shard_started, %{pid: pid}}, 5_000
    assert Process.alive?(pid) == true

    assert Enum.empty?(shards) == false
  end

  test "do multiple fetches of the stream description to get a full list of shards" do
    {:ok, _} =
      start_supervised({DynamicSupervisor, [strategy: :one_for_one, name: @supervisor_name]})

    opts = coordinator_opts()

    KinesisMock
    |> expect(:describe_stream, fn stream_name, _opts ->
      assert stream_name == @stream_name
      {:ok, KinesisClient.KinesisResponses.describe_stream(has_more_shards: true)}
    end)
    |> expect(:describe_stream, fn stream_name, _opts ->
      assert stream_name == @stream_name
      {:ok, KinesisClient.KinesisResponses.describe_stream(has_more_shards: false)}
    end)
    |> stub(:get_records, fn _, _ ->
      []
    end)

    AppStateMock
    |> expect(:initialize, fn in_app_name, _ ->
      assert in_app_name == opts[:app_name]
      :ok
    end)
    |> stub(:get_lease, fn _, _, _, _ ->
      :not_found
    end)
    |> stub(:create_lease, fn _, _, _, _, _ ->
      :ok
    end)

    {:ok, _} = start_coordinator(opts)

    assert_receive {:shards, shards}, 5_000
    assert_receive {:shard_started, %{pid: pid}}, 5_000
    assert Process.alive?(pid) == true

    assert Enum.empty?(shards) == false
  end

  @tag capture_log: true
  test "don't start child shard until parent shard is closed" do
    {:ok, _} =
      start_supervised({DynamicSupervisor, [strategy: :one_for_one, name: @supervisor_name]})

    opts = coordinator_opts()

    KinesisMock
    |> expect(:describe_stream, fn stream_name, _opts ->
      assert stream_name == @stream_name
      {:ok, KinesisClient.KinesisResponses.describe_stream_split()}
    end)
    |> stub(:get_records, fn _, _ ->
      []
    end)

    shard0_lease = %ShardLease{shard_id: "shardId-000000000000", completed: false}
    shard1_lease = %ShardLease{shard_id: "shardId-000000000001", completed: false}
    shard2_lease = %ShardLease{shard_id: "shardId-000000000002", completed: false}
    shard3_lease = %ShardLease{shard_id: "shardId-000000000003", completed: false}

    AppStateMock
    |> expect(:initialize, fn in_app_name, _ ->
      assert in_app_name == opts[:app_name]
      :ok
    end)
    |> stub(:get_lease, fn _, _, shard_id, _ ->
      case shard_id do
        "shardId-000000000000" -> shard0_lease
        "shardId-000000000001" -> shard1_lease
        "shardId-000000000002" -> shard2_lease
        "shardId-000000000003" -> shard3_lease
      end
    end)
    |> stub(:get_leases_by_worker, fn _in_app_name, _in_stream_name, _lease_owner, _ ->
      [shard0_lease, shard1_lease, shard2_lease, shard3_lease]
    end)

    {:ok, _} = start_coordinator(opts)

    assert_receive {:shards, shards}, 100
    assert_receive {:shard_started, %{pid: pid_0, shard_id: "shardId-000000000000"}}, 100
    assert Process.alive?(pid_0) == true
    assert_receive {:shard_started, %{pid: pid_1, shard_id: "shardId-000000000001"}}, 100
    assert Process.alive?(pid_1) == true

    refute_receive {:shard_started, %{pid: _, shard_id: "shardId-000000000002"}}, 200
    assert Process.alive?(pid_0) == true
    refute_receive {:shard_started, %{pid: _, shard_id: "shardId-000000000003"}}, 200
    assert Process.alive?(pid_1) == true

    assert Enum.empty?(shards) == false
  end

  @tag capture_log: true
  test "start child shard if parent is marked completed" do
    {:ok, _} =
      start_supervised({DynamicSupervisor, [strategy: :one_for_one, name: @supervisor_name]})

    opts = coordinator_opts()

    KinesisMock
    |> expect(:describe_stream, fn stream_name, _opts ->
      assert stream_name == @stream_name
      {:ok, KinesisClient.KinesisResponses.describe_stream_split()}
    end)
    |> stub(:get_records, fn _, _ ->
      []
    end)

    shard0_lease = %ShardLease{shard_id: "shardId-000000000000", completed: true}
    shard1_lease = %ShardLease{shard_id: "shardId-000000000001", completed: false}
    shard2_lease = %ShardLease{shard_id: "shardId-000000000002", completed: false}
    shard3_lease = %ShardLease{shard_id: "shardId-000000000003", completed: false}

    AppStateMock
    |> expect(:initialize, fn in_app_name, _ ->
      assert in_app_name == opts[:app_name]
      :ok
    end)
    |> stub(:get_lease, fn _, _, shard_id, _ ->
      case shard_id do
        "shardId-000000000000" -> shard0_lease
        "shardId-000000000001" -> shard1_lease
        "shardId-000000000002" -> shard2_lease
        "shardId-000000000003" -> shard3_lease
      end
    end)

    {:ok, _} = start_coordinator(opts)

    assert_receive {:shards, shards}, 100
    assert_receive {:shard_started, %{pid: _, shard_id: "shardId-000000000001"}}, 100
    assert_receive {:shard_started, %{pid: _, shard_id: "shardId-000000000002"}}, 100
    assert_receive {:shard_started, %{pid: _, shard_id: "shardId-000000000003"}}, 100

    refute_receive {:shard_started, %{pid: _, shard_id: "shardId-000000000000"}}, 100

    assert Enum.empty?(shards) == false
  end

  test "will retry initialization after :retry_timeout if stream status not ACTIVE" do
    {:ok, _} =
      start_supervised({DynamicSupervisor, [strategy: :one_for_one, name: @supervisor_name]})

    opts = coordinator_opts(retry_timeout: 100)

    KinesisMock
    |> expect(:describe_stream, fn stream_name, _opts ->
      assert stream_name == @stream_name
      {:ok, KinesisClient.KinesisResponses.describe_stream(stream_status: "UPDATING")}
    end)
    |> stub(:get_records, fn _, _ ->
      []
    end)

    AppStateMock
    |> expect(:initialize, fn in_app_name, _ ->
      assert in_app_name == opts[:app_name]
      :ok
    end)
    |> stub(:get_lease, fn _, _, _, _ ->
      :not_found
    end)
    |> stub(:create_lease, fn _, _, _, _, _ ->
      :ok
    end)

    {:ok, coordinator} = start_coordinator(opts)
    assert_receive {:retrying_describe_stream, pid}, 5_000

    assert coordinator == pid
  end

  defp coordinator_opts(overrides \\ []) do
    coordinator_name = MyTestCoordinator
    app_name = "uberbrodt-kinesis-client-test-app"

    opts = [
      name: coordinator_name,
      app_name: app_name,
      app_state_opts: [adapter: :test],
      stream_name: @stream_name,
      shard_supervisor_name: @supervisor_name,
      notify_pid: self(),
      kinesis_opts: [adapter: KinesisClient.KinesisMock],
      shard_args: [
        shard_consumer: __MODULE__,
        app_name: app_name,
        coordinator_name: coordinator_name,
        lease_owner: worker_ref(),
        pipeline: KinesisClient.TestPipeline,
        app_state_opts: [adapter: :test],
        processors: [
          default: [
            concurrency: 1,
            min_demand: 10,
            max_demand: 20
          ]
        ],
        batchers: [
          default: [
            concurrency: 1,
            batch_size: 40
          ]
        ]
      ]
    ]

    Keyword.merge(opts, overrides)
  end

  defp start_coordinator(args) do
    start_supervised(Supervisor.child_spec({Coordinator, args}, restart: :temporary))
  end
end
