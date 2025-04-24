defmodule KinesisClient.Stream.Shard.ProducerTest do
  use KinesisClient.Case

  alias KinesisClient.Stream.Shard.Producer

  test "returns messages in response to demand if status is not :stopped" do
    opts = producer_opts(status: :started)
    {:ok, producer} = start_supervised({Producer, opts})
    {:ok, consumer} = start_supervised({KinesisClient.TestConsumer, self()})

    KinesisMock
    |> expect(:get_shard_iterator, fn _, _, _, _ ->
      {:ok, %{"ShardIterator" => "somesharditerator"}}
    end)
    |> expect(:get_records, fn _, _ ->
      records = [
        %{"Data" => "foo", "SequenceNumber" => "12345"}
      ]

      {:ok, %{"NextShardIterator" => "foo", "MillisBehindLatest" => 5_000, "Records" => records}}
    end)

    AppStateMock
    |> stub(:get_lease, fn _in_app_name, _in_stream_name, _in_shard_id, _ ->
      %{lease_owner: opts[:lease_owner]}
    end)

    GenStage.sync_subscribe(consumer, to: producer)
    assert_receive {:consumer_events, [record]}, 1_000

    assert record.data == "foo"
  end

  test "stores demand if :status == :stopped" do
    opts = producer_opts()
    {:ok, producer} = start_supervised({Producer, opts})
    {:ok, consumer} = start_supervised({KinesisClient.TestConsumer, self()})
    GenStage.sync_subscribe(consumer, to: producer)

    assert Process.alive?(producer)
    assert_receive {:queuing_demand_while_stopped, _}, 1_000
    refute_receive {:consumer_events, _}, 1_000
  end

  test "stores partial demand if cannot totally fulfill consumer request" do
    opts = producer_opts(status: :started)
    {:ok, producer} = start_supervised({Producer, opts})
    {:ok, consumer} = start_supervised({KinesisClient.TestConsumer, self()})

    KinesisMock
    |> expect(:get_shard_iterator, fn _, _, _, _ ->
      {:ok, %{"ShardIterator" => "somesharditerator"}}
    end)
    |> expect(:get_records, fn _, opts ->
      count = opts[:limit] - 5
      records = Enum.map(0..count, fn _ -> %{"Data" => "foo", "SequenceNumber" => "12345"} end)

      {:ok, %{"NextShardIterator" => "foo", "MillisBehindLatest" => 1_000, "Records" => records}}
    end)

    AppStateMock
    |> stub(:get_lease, fn _in_app_name, _in_stream_name, _in_shard_id, _ ->
      %{lease_owner: opts[:lease_owner]}
    end)

    GenStage.sync_subscribe(consumer, to: producer, max_demand: 10, min_demand: 0)
    assert_receive {:consumer_events, _}, 1_000
    assert_receive :poll_timer_executed, 2_000
  end

  test "checkpoints ShardLease with sequence_number from latest successful msgs" do
    opts = producer_opts(status: :started)
    {:ok, producer} = start_supervised({Producer, opts})
    {:ok, consumer} = start_supervised({KinesisClient.TestConsumer, self()})

    KinesisMock
    |> expect(:get_shard_iterator, fn _, _, _, _ ->
      {:ok, %{"ShardIterator" => "somesharditerator"}}
    end)
    |> expect(:get_records, fn _, opts ->
      count = opts[:limit] - 5
      records = Enum.map(0..count, fn _ -> %{"Data" => "foo", "SequenceNumber" => "12345"} end)

      {:ok, %{"NextShardIterator" => "foo", "MillisBehindLatest" => 1_000, "Records" => records}}
    end)

    expect(AppStateMock, :update_checkpoint, fn in_app_name,
                                                in_stream_name,
                                                in_shard_id,
                                                in_lease_owner,
                                                in_checkpoint,
                                                _opts ->
      assert in_app_name == opts[:app_name]
      assert in_stream_name == opts[:stream_name]
      assert in_shard_id == opts[:shard_id]
      assert in_lease_owner == opts[:lease_owner]
      assert in_checkpoint == "12345"
      :ok
    end)

    AppStateMock
    |> stub(:get_lease, fn _in_app_name, _in_stream_name, _in_shard_id, _ ->
      %{lease_owner: opts[:lease_owner]}
    end)

    GenStage.sync_subscribe(consumer, to: producer, max_demand: 10, min_demand: 0)
    assert_receive {:consumer_events, events}, 1_000

    send(producer, {:ack, make_ref(), events, []})

    assert_receive {:acked, %{success: _successful, checkpoint: "12345", failed: []}}, 10_000
  end

  test "close the shard when getting ResourceNotFoundException error" do
    opts = producer_opts(status: :started)
    {:ok, producer} = start_supervised({Producer, opts})
    {:ok, consumer} = start_supervised({KinesisClient.TestConsumer, self()})

    KinesisMock
    |> expect(:get_shard_iterator, fn _, _, _, _opts ->
      {:error, {"ResourceNotFoundException", "ResourceNotFoundException error"}}
    end)

    AppStateMock
    |> expect(:close_shard, fn in_app_name, in_stream_name, in_shard_id, in_lease_owner, _opts ->
      assert in_app_name == opts[:app_name]
      assert in_stream_name == opts[:stream_name]
      assert in_shard_id == opts[:shard_id]
      assert in_lease_owner == opts[:lease_owner]
      :ok
    end)

    GenStage.sync_subscribe(consumer, to: producer)

    send(producer, :shard_closed)

    assert_receive {:shard_closed, state}, 10_000
    assert state.status == :closed
  end

  describe "test demand_limit in the state" do
    test "demand_limit is set correctly in the state when initializing" do
      opts = producer_opts(kinesis_opts: [limit: 1000])

      {:ok, _producer} = start_supervised({Producer, opts})

      assert_receive {:init, state}, 1_000
      assert state.demand_limit == 1000
    end

    test "demand_limit is set to default when option is not given" do
      opts = producer_opts()

      {:ok, _producer} = start_supervised({Producer, opts})

      assert_receive {:init, state}, 1_000
      assert state.demand_limit == 500
    end
  end

  defp producer_opts(overrides \\ []) do
    opts = [
      app_name: "foo",
      shard_id: "shardId-000000000000",
      stream_name: "kcl-ex-test-stream",
      kinesis_opts: [adapter: KinesisMock],
      app_state_opts: [adapter: :test],
      status: :stopped,
      lease_owner: worker_ref(),
      notify_pid: self()
    ]

    Keyword.merge(opts, overrides)
  end
end
