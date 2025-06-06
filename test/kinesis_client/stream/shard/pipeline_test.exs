defmodule KinesisClient.Stream.Shard.PipelineTest do
  use KinesisClient.Case, async: false

  alias KinesisClient.Stream.AppState.ShardLease
  alias KinesisClient.Stream.Shard.Pipeline

  test "can start producer" do
    app_name = "sdf9023kl"
    stream_name = "pipeline-test-stream"
    shard_id = "shard-1"

    opts = [
      app_name: app_name,
      stream_name: stream_name,
      app_state_opts: [adapter: :test],
      shard_id: shard_id,
      kinesis_opts: [adapter: KinesisMock],
      shard_consumer: KinesisClient.TestShardConsumer,
      stream_name: "pipeline-test-stream",
      poll_interval: 60_000,
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

    KinesisMock
    |> stub(:get_shard_iterator, fn in_stream_name, in_shard_id, in_shard_iterator_type, _ ->
      assert in_stream_name == opts[:stream_name]
      assert in_shard_id == opts[:shard_id]
      assert in_shard_iterator_type == :trim_horizon
      {:ok, %{"ShardIterator" => "foo"}}
    end)
    |> stub(:get_records, fn iterator, _opts ->
      assert iterator != nil

      {:ok,
       %{
         "NextShardIterator" => "foo",
         "MillisBehindLatest" => 100,
         "Records" =>
           Enum.map(0..19, fn _ ->
             %{"Data" => "", "PartitionKey" => "3qwc3", "SequenceNumber" => "12345"}
           end)
       }}
    end)

    expect(AppStateMock, :get_lease, 2, fn _, _, _, _ ->
      %ShardLease{checkpoint: nil}
    end)

    {:ok, pid} = start_supervised({Pipeline, opts})

    assert Process.alive?(pid)

    assert :ok ==
             Pipeline.start(%{app_name: app_name, stream_name: stream_name, shard_id: shard_id})
  end

  test "can stop producer" do
    app_name = "sdf9023kl"
    stream_name = "pipeline-test-stream"
    shard_id = "shard-1"

    opts = [
      app_name: app_name,
      stream_name: stream_name,
      shard_id: shard_id,
      shard_consumer: __MODULE__
    ]

    {:ok, pid} = start_supervised({Pipeline, opts})

    assert Process.alive?(pid)

    assert :ok == Pipeline.stop(%{app_name: app_name, stream_name: stream_name, shard_id: shard_id})
  end
end
