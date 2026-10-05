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

  test "stops producing when the checkpoint fails because the lease was lost" do
    opts = producer_opts(status: :started)
    {:ok, producer} = start_supervised({Producer, opts})
    {:ok, consumer} = start_supervised({KinesisClient.TestConsumer, self()})

    KinesisMock
    |> expect(:get_shard_iterator, fn _, _, _, _ ->
      {:ok, %{"ShardIterator" => "somesharditerator"}}
    end)
    |> expect(:get_records, fn _, _ ->
      records = [%{"Data" => "foo", "SequenceNumber" => "12345"}]

      {:ok, %{"NextShardIterator" => "foo", "MillisBehindLatest" => 1_000, "Records" => records}}
    end)

    # We own the lease for the fetch, then another worker steals it before
    # the checkpoint: the owner-guarded checkpoint fails and get_lease names
    # the thief.
    AppStateMock
    |> expect(:get_lease, fn _in_app_name, _in_stream_name, _in_shard_id, _ ->
      %{lease_owner: opts[:lease_owner]}
    end)
    |> stub(:get_lease, fn _in_app_name, _in_stream_name, _in_shard_id, _ ->
      %{lease_owner: worker_ref()}
    end)
    |> stub(:update_checkpoint, fn _app_name, _stream_name, _shard_id, _owner, _checkpoint, _ ->
      {:error, :lease_owner_match}
    end)

    GenStage.sync_subscribe(consumer, to: producer, max_demand: 10, min_demand: 0)
    assert_receive {:consumer_events, events}, 1_000

    send(producer, {:ack, make_ref(), events, []})

    assert_receive {:lease_lost, _shard_id}, 1_000
    assert :sys.get_state(producer).state.status == :stopped
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

  describe "start/1" do
    test "replies before fetching so a slow Kinesis call cannot time out the caller" do
      opts = producer_opts()
      {:ok, producer} = start_supervised({Producer, opts})

      AppStateMock
      |> stub(:get_lease, fn _app_name, _stream_name, _shard_id, _opts ->
        %{checkpoint: nil}
      end)

      test_pid = self()

      # Blocks inside the fetch until the test releases it, standing in for
      # Kinesis I/O that outlives GenServer.call/2's default 5s timeout — the
      # fetch is wrapped in @retry with exponential backoff, so ~15s is
      # reachable. When the reply was sent after the fetch, this deadlocked
      # start/1 and killed the calling LeaseV2 process.
      KinesisMock
      |> stub(:get_shard_iterator, fn _, _, _, _ ->
        send(test_pid, {:fetch_started, self()})

        receive do
          :release -> :ok
        after
          10_000 -> :timeout
        end

        {:ok, %{"ShardIterator" => "somesharditerator"}}
      end)
      |> stub(:get_records, fn _, _ ->
        {:ok, %{"NextShardIterator" => "foo", "MillisBehindLatest" => 0, "Records" => []}}
      end)

      {elapsed_us, reply} = :timer.tc(fn -> Producer.start(producer) end)

      assert reply == :ok
      assert elapsed_us < 2_000_000, "start/1 waited #{div(elapsed_us, 1000)}ms for the fetch"

      # The fetch is still in flight, which is the point: the reply did not wait.
      assert_receive {:fetch_started, producer_pid}, 1_000
      send(producer_pid, :release)
    end
  end

  describe "status/1" do
    test "reports :stopped until started, then :started" do
      opts = producer_opts()
      {:ok, producer} = start_supervised({Producer, opts})

      assert Producer.status(producer) == :stopped

      AppStateMock
      |> stub(:get_lease, fn _app_name, _stream_name, _shard_id, _opts ->
        %{lease_owner: opts[:lease_owner], checkpoint: nil}
      end)

      KinesisMock
      |> stub(:get_shard_iterator, fn _, _, _, _ -> {:ok, %{"ShardIterator" => "iterator"}} end)
      |> stub(:get_records, fn _, _ ->
        {:ok, %{"NextShardIterator" => "next", "MillisBehindLatest" => 0, "Records" => []}}
      end)

      assert :ok == Producer.start(producer)
      assert Producer.status(producer) == :started
    end
  end

  # TD-6631: a DBConnection.ConnectionError raised from the lease lookup
  # crashed the producer; Broadway restarted it as :stopped and the shard sat
  # idle until its lease moved. Lease lookups must never take the producer down.
  describe "lease lookup failures" do
    test ":start stays stopped and alive when the lease lookup raises" do
      opts = producer_opts()
      {:ok, producer} = start_supervised({Producer, opts})

      AppStateMock
      |> expect(:get_lease, fn _app_name, _stream_name, _shard_id, _opts ->
        raise DBConnection.ConnectionError, "connection not available"
      end)

      assert :ok == Producer.start(producer)
      assert Producer.status(producer) == :stopped
      assert Process.alive?(producer)
    end

    test "poll tick polls again instead of crashing when the lease lookup fails" do
      opts = producer_opts(status: :started, poll_interval: 50)
      {:ok, producer} = start_supervised({Producer, opts})

      AppStateMock
      |> expect(:get_lease, fn _app_name, _stream_name, _shard_id, _opts ->
        {:error, :timeout}
      end)
      |> expect(:get_lease, fn _app_name, _stream_name, _shard_id, _opts ->
        raise DBConnection.ConnectionError, "connection not available"
      end)
      |> stub(:get_lease, fn _app_name, _stream_name, _shard_id, _opts ->
        %{lease_owner: worker_ref()}
      end)

      # A poll tick is only honoured while a poll timer is recorded, so stand
      # in for the (already fired) timer that delivered this tick.
      :sys.replace_state(producer, fn %{state: state} = stage ->
        fired_timer = Process.send_after(self(), :noop, 60_000)
        Process.cancel_timer(fired_timer)
        %{stage | state: %{state | poll_timer: fired_timer}}
      end)

      send(producer, :get_records)

      # Three ticks: the original plus one rescheduled after each failure. The
      # third sees another owner and stops polling without a Kinesis call.
      assert_receive :poll_timer_executed, 1_000
      assert_receive :poll_timer_executed, 1_000
      assert_receive :poll_timer_executed, 1_000
      refute_receive :poll_timer_executed, 200
      assert Process.alive?(producer)
    end

    test "poll tick with no pending demand waits for demand instead of fetching" do
      opts = producer_opts(status: :started, poll_interval: 50)
      {:ok, producer} = start_supervised({Producer, opts})

      # No KinesisMock expectations: a fetch with limit: 0 would be an
      # unexpected call and crash the producer.
      AppStateMock
      |> expect(:get_lease, fn _app_name, _stream_name, _shard_id, _opts ->
        %{lease_owner: opts[:lease_owner]}
      end)

      :sys.replace_state(producer, fn %{state: state} = stage ->
        fired_timer = Process.send_after(self(), :noop, 60_000)
        Process.cancel_timer(fired_timer)
        %{stage | state: %{state | poll_timer: fired_timer}}
      end)

      send(producer, :get_records)

      assert_receive :poll_timer_executed, 1_000
      refute_receive :poll_timer_executed, 200
      assert Process.alive?(producer)
      assert :sys.get_state(producer).state.poll_timer == nil
    end

    test "fetch path polls again after poll_interval instead of blocking in retries" do
      opts = producer_opts(status: :started, poll_interval: 50)
      {:ok, producer} = start_supervised({Producer, opts})
      {:ok, consumer} = start_supervised({KinesisClient.TestConsumer, self()})

      KinesisMock
      |> stub(:get_shard_iterator, fn _, _, _, _ -> {:ok, %{"ShardIterator" => "iterator"}} end)

      # First ownership check (inside the fetch) fails; the next poll sees
      # another owner and goes quiet.
      AppStateMock
      |> expect(:get_lease, fn _app_name, _stream_name, _shard_id, _opts -> {:error, :timeout} end)
      |> stub(:get_lease, fn _app_name, _stream_name, _shard_id, _opts ->
        %{lease_owner: worker_ref()}
      end)

      {elapsed_us, _} =
        :timer.tc(fn ->
          GenStage.sync_subscribe(consumer, to: producer, max_demand: 10, min_demand: 0)
          assert_receive :poll_timer_executed, 1_000
        end)

      # Well under the ~15s the @retry backoff would have taken.
      assert elapsed_us < 1_000_000
      refute_receive {:consumer_events, _}, 100
      assert Process.alive?(producer)
    end

    test "keeps retrying failed messages while stopped when ownership cannot be verified" do
      opts = producer_opts()
      {:ok, producer} = start_supervised({Producer, opts})
      {:ok, consumer} = start_supervised({KinesisClient.TestConsumer, self()})

      AppStateMock
      |> expect(:get_lease, fn _app_name, _stream_name, _shard_id, _opts ->
        raise DBConnection.ConnectionError, "connection not available"
      end)

      GenStage.sync_subscribe(consumer, to: producer)
      assert_receive {:queuing_demand_while_stopped, _}, 1_000

      failed = [
        %Broadway.Message{data: "retry-me", acknowledger: {Broadway.NoopAcknowledger, nil, nil}}
      ]

      send(producer, {:ack, make_ref(), [], failed})

      assert_receive {:consumer_events, ^failed}, 1_000
      assert Process.alive?(producer)
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
