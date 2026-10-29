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

  # TD-6722: a shard iterator expires after 5 minutes without a GetRecords
  # call. Keeping the stale iterator made every later poll fail the same way,
  # forever, with no checkpoint movement and no crash to restart from.
  describe "ExpiredIteratorException" do
    test "re-derives the iterator from the last checkpoint and keeps consuming" do
      opts = producer_opts(poll_interval: 50)
      {:ok, producer} = start_supervised({Producer, opts})
      {:ok, consumer} = start_supervised({KinesisClient.TestConsumer, self()})

      # Started from a lease checkpointed at "100"; the first iterator has
      # expired by the time it is used.
      KinesisMock
      |> expect(:get_shard_iterator, fn _, _, :after_sequence_number, kinesis_opts ->
        assert kinesis_opts[:starting_sequence_number] == "100"
        {:ok, %{"ShardIterator" => "iterator-1"}}
      end)
      |> expect(:get_records, fn "iterator-1", _ ->
        {:error, {"ExpiredIteratorException", "Iterator expired."}}
      end)
      |> expect(:get_shard_iterator, fn _, _, :after_sequence_number, kinesis_opts ->
        assert kinesis_opts[:starting_sequence_number] == "100"
        {:ok, %{"ShardIterator" => "iterator-2"}}
      end)
      |> expect(:get_records, fn "iterator-2", _ ->
        records = [%{"Data" => "foo", "SequenceNumber" => "101"}]
        {:ok, %{"NextShardIterator" => "iterator-3", "Records" => records}}
      end)
      |> stub(:get_records, fn iterator, _ ->
        {:ok, %{"NextShardIterator" => iterator, "Records" => []}}
      end)

      AppStateMock
      |> stub(:get_lease, fn _, _, _, _ ->
        %{lease_owner: opts[:lease_owner], checkpoint: "100", completed: false}
      end)
      |> stub(:update_checkpoint, fn _, _, _, _, _, _ -> :ok end)

      assert Producer.start(producer) == :ok

      # Expiry must not block in @retry backoff (~15s) before the next fetch.
      {elapsed_us, msg} =
        :timer.tc(fn ->
          GenStage.sync_subscribe(consumer, to: producer, max_demand: 1, min_demand: 0)

          assert_receive {:consumer_events, [%{metadata: %{"SequenceNumber" => "101"}} = msg]},
                         2_000

          msg
        end)

      assert elapsed_us < 1_500_000

      # A later ack moves the point the next expiry would restart from.
      send(producer, {:ack, make_ref(), [msg], []})
      assert_receive {:acked, %{checkpoint: "101"}}, 1_000
      assert :sys.get_state(producer).state.last_checkpoint == "101"
      assert Process.alive?(producer)
    end

    test "re-derives the iterator from the configured start when nothing is checkpointed" do
      opts = producer_opts(status: :started, poll_interval: 50, shard_iterator_type: :latest)
      {:ok, producer} = start_supervised({Producer, opts})
      {:ok, consumer} = start_supervised({KinesisClient.TestConsumer, self()})

      KinesisMock
      |> expect(:get_shard_iterator, fn _, _, :latest, _ ->
        {:ok, %{"ShardIterator" => "iterator-1"}}
      end)
      |> expect(:get_records, fn "iterator-1", _ ->
        {:error, {"ExpiredIteratorException", "Iterator expired."}}
      end)
      |> expect(:get_shard_iterator, fn _, _, :latest, _ ->
        {:ok, %{"ShardIterator" => "iterator-2"}}
      end)
      |> expect(:get_records, fn "iterator-2", _ ->
        {:ok,
         %{
           "NextShardIterator" => "iterator-3",
           "Records" => [%{"Data" => "foo", "SequenceNumber" => "1"}]
         }}
      end)

      AppStateMock
      |> stub(:get_lease, fn _, _, _, _ -> %{lease_owner: opts[:lease_owner]} end)

      GenStage.sync_subscribe(consumer, to: producer, max_demand: 1, min_demand: 0)

      assert_receive {:consumer_events, [%{metadata: %{"SequenceNumber" => "1"}}]}, 2_000
      assert Process.alive?(producer)
    end
  end

  # TD-6722: the GetRecords response that carries ChildShards is the last one
  # for the shard. Marking the lease completed before those records are acked
  # loses them if the worker dies in between: no worker reads a completed
  # shard again.
  describe "end of shard" do
    setup do
      opts = producer_opts(status: :started, poll_interval: 50)
      {:ok, producer} = start_supervised({Producer, opts})
      {:ok, consumer} = start_supervised({KinesisClient.TestConsumer, self()})

      test_pid = self()

      AppStateMock
      |> stub(:get_lease, fn _, _, _, _ -> %{lease_owner: opts[:lease_owner]} end)
      |> stub(:update_checkpoint, fn _, _, _, _, _, _ -> :ok end)
      |> stub(:close_shard, fn _, _, _, _, _ ->
        send(test_pid, :close_shard_called)
        :ok
      end)

      KinesisMock
      |> stub(:get_shard_iterator, fn _, _, _, _ -> {:ok, %{"ShardIterator" => "iterator"}} end)

      %{producer: producer, consumer: consumer}
    end

    test "marks the shard completed only once every tail record is acked", ctx do
      records = [
        %{"Data" => "a", "SequenceNumber" => "1"},
        %{"Data" => "b", "SequenceNumber" => "2"}
      ]

      expect(KinesisMock, :get_records, fn _, _ ->
        {:ok, %{"ChildShards" => [], "Records" => records}}
      end)

      GenStage.sync_subscribe(ctx.consumer, to: ctx.producer, max_demand: 10, min_demand: 0)
      assert_receive {:consumer_events, [msg_a, msg_b]}, 1_000
      assert :sys.get_state(ctx.producer).state.status == :closed
      refute_receive :close_shard_called, 200

      # Acked in two batches, as Broadway does
      send(ctx.producer, {:ack, make_ref(), [msg_a], []})
      assert_receive {:acked, %{checkpoint: "1"}}, 1_000
      refute_receive :close_shard_called, 200

      send(ctx.producer, {:ack, make_ref(), [msg_b], []})
      assert_receive {:acked, %{checkpoint: "2"}}, 1_000
      assert_receive :close_shard_called, 1_000
    end

    test "waits for records still in flight from earlier fetches", ctx do
      KinesisMock
      |> expect(:get_records, fn _, _ ->
        {:ok,
         %{"NextShardIterator" => "next", "Records" => [%{"Data" => "a", "SequenceNumber" => "1"}]}}
      end)
      |> expect(:get_records, fn _, _ -> {:ok, %{"ChildShards" => [], "Records" => []}} end)

      GenStage.sync_subscribe(ctx.consumer, to: ctx.producer, max_demand: 10, min_demand: 0)
      assert_receive {:consumer_events, [msg_a]}, 1_000

      # The empty final response arrives on the follow-up poll
      assert_receive :poll_timer_executed, 1_000
      refute_receive :close_shard_called, 200
      assert :sys.get_state(ctx.producer).state.status == :closed

      send(ctx.producer, {:ack, make_ref(), [msg_a], []})
      assert_receive :close_shard_called, 1_000
    end

    test "a failed tail record is retried before the shard is closed", ctx do
      records = [
        %{"Data" => "a", "SequenceNumber" => "1"},
        %{"Data" => "b", "SequenceNumber" => "2"}
      ]

      expect(KinesisMock, :get_records, fn _, _ ->
        {:ok, %{"ChildShards" => [], "Records" => records}}
      end)

      GenStage.sync_subscribe(ctx.consumer, to: ctx.producer, max_demand: 10, min_demand: 0)
      assert_receive {:consumer_events, [msg_a, msg_b]}, 1_000

      send(ctx.producer, {:ack, make_ref(), [msg_a], [msg_b]})
      assert_receive {:consumer_events, [^msg_b]}, 1_000
      refute_receive :close_shard_called, 200

      send(ctx.producer, {:ack, make_ref(), [msg_b], []})
      assert_receive :close_shard_called, 1_000
    end

    test "closes right away when the final response carries no records", ctx do
      expect(KinesisMock, :get_records, fn _, _ ->
        {:ok, %{"ChildShards" => [], "Records" => []}}
      end)

      GenStage.sync_subscribe(ctx.consumer, to: ctx.producer, max_demand: 10, min_demand: 0)
      assert_receive :close_shard_called, 1_000
    end
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

      KinesisMock
      |> stub(:get_shard_iterator, fn _, _, _, _ -> {:ok, %{"ShardIterator" => "iterator"}} end)

      # A poll tick is only honoured while a poll timer is recorded, so stand
      # in for the (already fired) timer that delivered this tick. The lease is
      # read by the fetch, which only runs with demand pending.
      :sys.replace_state(producer, fn %{state: state} = stage ->
        fired_timer = Process.send_after(self(), :noop, 60_000)
        Process.cancel_timer(fired_timer)
        %{stage | state: %{state | poll_timer: fired_timer, demand: 10}}
      end)

      send(producer, :get_records)

      # Three ticks: the original plus one rescheduled after each failure. The
      # third sees another owner, which also polls again (no Kinesis call).
      assert_receive :poll_timer_executed, 1_000
      assert_receive :poll_timer_executed, 1_000
      assert_receive :poll_timer_executed, 1_000
      assert Process.alive?(producer)
    end

    test "poll tick keeps polling when the lease names another worker" do
      opts = producer_opts(status: :started, poll_interval: 50)
      {:ok, producer} = start_supervised({Producer, opts})
      {:ok, consumer} = start_supervised({KinesisClient.TestConsumer, self()})

      # A stale :not_owner reading (Dynamo get_item is eventually consistent)
      # must not leave a :started producer idle with no poll timer — that is
      # the TD-6631 stall again, and invisible to Pipeline.stopped?/1. No
      # KinesisMock expectations: fetching while not owner would crash.
      AppStateMock
      |> expect(:get_lease, 2, fn _app_name, _stream_name, _shard_id, _opts ->
        %{lease_owner: worker_ref()}
      end)
      |> stub(:get_lease, fn _app_name, _stream_name, _shard_id, _opts ->
        %{lease_owner: opts[:lease_owner], checkpoint: nil}
      end)

      KinesisMock
      |> stub(:get_shard_iterator, fn _, _, _, _ -> {:ok, %{"ShardIterator" => "iterator"}} end)
      |> stub(:get_records, fn _, _ ->
        records = [%{"Data" => "after-stale-read", "SequenceNumber" => "1"}]
        {:ok, %{"NextShardIterator" => "next", "MillisBehindLatest" => 0, "Records" => records}}
      end)

      # Demand arrives first; the fetch sees the stale owner and must poll again
      # instead of idling. Once the row reads as ours, records flow.
      GenStage.sync_subscribe(consumer, to: producer, max_demand: 10, min_demand: 0)

      assert_receive :poll_timer_executed, 1_000
      assert_receive :poll_timer_executed, 1_000
      assert_receive {:consumer_events, [record]}, 1_000
      assert record.data == "after-stale-read"
      assert Process.alive?(producer)
    end

    test "poll tick with no pending demand waits for demand instead of fetching" do
      opts = producer_opts(status: :started, poll_interval: 50)
      {:ok, producer} = start_supervised({Producer, opts})

      # No KinesisMock expectations: a fetch with limit: 0 would be an
      # unexpected call and crash the producer. Nothing to fetch also means
      # nothing to check the lease for.
      expect(AppStateMock, :get_lease, 0, fn _, _, _, _ -> flunk("lease read") end)

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

    test "survives a checkpoint write that raises and keeps producing" do
      opts = producer_opts(status: :started)
      {:ok, producer} = start_supervised({Producer, opts})
      {:ok, consumer} = start_supervised({KinesisClient.TestConsumer, self()})

      # One batch, then empty polls: the producer keeps polling after the
      # failed checkpoint, which is the point.
      KinesisMock
      |> stub(:get_shard_iterator, fn _, _, _, _ -> {:ok, %{"ShardIterator" => "iterator"}} end)
      |> expect(:get_records, fn _, _ ->
        records = [%{"Data" => "foo", "SequenceNumber" => "12345"}]
        {:ok, %{"NextShardIterator" => "next", "MillisBehindLatest" => 0, "Records" => records}}
      end)
      |> stub(:get_records, fn _, _ ->
        {:ok, %{"NextShardIterator" => "next", "MillisBehindLatest" => 0, "Records" => []}}
      end)

      # The checkpoint write hits an exhausted pool (the Ecto adapter raises),
      # and the follow-up lease read fails the same way. Before the guard the
      # producer crashed here and came back :stopped.
      AppStateMock
      |> stub(:get_lease, fn _app_name, _stream_name, _shard_id, _opts ->
        %{lease_owner: opts[:lease_owner]}
      end)
      |> expect(:update_checkpoint, fn _app_name, _stream_name, _shard_id, _owner, _cp, _opts ->
        raise DBConnection.ConnectionError, "connection not available"
      end)

      GenStage.sync_subscribe(consumer, to: producer, max_demand: 10, min_demand: 0)
      assert_receive {:consumer_events, events}, 1_000

      send(producer, {:ack, make_ref(), events, []})

      refute_receive {:acked, _}, 300
      assert Process.alive?(producer)
      assert Producer.status(producer) == :started
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

  describe "poll scheduling" do
    setup do
      test_pid = self()

      KinesisMock
      |> stub(:get_shard_iterator, fn _, _, _, _ -> {:ok, %{"ShardIterator" => "iterator"}} end)
      |> stub(:get_records, fn _, _ ->
        send(test_pid, :fetched)
        {:ok, %{"NextShardIterator" => "next", "MillisBehindLatest" => 0, "Records" => []}}
      end)

      :ok
    end

    test "new demand while a poll is pending does not start a second poll loop" do
      opts = producer_opts(status: :started, poll_interval: 100)
      {:ok, producer} = start_supervised({Producer, opts})
      {:ok, first} = start_supervised({KinesisClient.TestConsumer, self()}, id: :first)
      {:ok, second} = start_supervised({KinesisClient.TestConsumer, self()}, id: :second)

      AppStateMock
      |> stub(:get_lease, fn _app_name, _stream_name, _shard_id, _opts ->
        %{lease_owner: opts[:lease_owner]}
      end)

      # Each subscription delivers demand, and each demand fetches right away
      # and arms a poll. Before the fix the second arm left the first timer
      # running, so the shard was polled by two self-renewing loops.
      GenStage.sync_subscribe(first, to: producer, max_demand: 10, min_demand: 0)
      assert_receive :fetched, 1_000
      GenStage.sync_subscribe(second, to: producer, max_demand: 10, min_demand: 0)
      assert_receive :fetched, 1_000

      flush(:poll_timer_executed)
      Process.sleep(1_050)

      # One loop at 100ms ticks ~10 times in this window; two would tick ~20.
      assert count(:poll_timer_executed) in 8..12
    end

    test "a stray tick while the recorded poll is still pending is ignored" do
      opts = producer_opts(status: :started, poll_interval: 60_000)
      {:ok, producer} = start_supervised({Producer, opts})

      # No AppState or Kinesis expectations are consumed: honouring the tick
      # would read the lease and fetch.
      expect(AppStateMock, :get_lease, 0, fn _, _, _, _ -> flunk("lease read") end)

      :sys.replace_state(producer, fn %{state: state} = stage ->
        pending = Process.send_after(self(), :noop, 60_000)
        %{stage | state: %{state | poll_timer: pending, demand: 10}}
      end)

      send(producer, :get_records)

      refute_receive :poll_timer_executed, 200
      refute_received :fetched
      assert Process.alive?(producer)
    end

    test "reads the lease once per poll" do
      test_pid = self()
      opts = producer_opts(status: :started, poll_interval: 50)
      {:ok, producer} = start_supervised({Producer, opts})
      {:ok, consumer} = start_supervised({KinesisClient.TestConsumer, self()})

      AppStateMock
      |> stub(:get_lease, fn _app_name, _stream_name, _shard_id, _opts ->
        send(test_pid, :lease_read)
        %{lease_owner: opts[:lease_owner]}
      end)

      GenStage.sync_subscribe(consumer, to: producer, max_demand: 10, min_demand: 0)
      Process.sleep(500)
      stop_supervised!(Producer)
      refute Process.alive?(producer)

      fetches = count(:fetched)
      lease_reads = count(:lease_read)

      assert fetches >= 5
      # A poll stopped between its lease read and its fetch leaves one extra.
      assert lease_reads in fetches..(fetches + 1)
    end

    test "max_poll_interval backs off consecutive empty polls" do
      opts = producer_opts(status: :started, poll_interval: 20, max_poll_interval: 160)
      {:ok, producer} = start_supervised({Producer, opts})
      {:ok, consumer} = start_supervised({KinesisClient.TestConsumer, self()})

      AppStateMock
      |> stub(:get_lease, fn _app_name, _stream_name, _shard_id, _opts ->
        %{lease_owner: opts[:lease_owner]}
      end)

      GenStage.sync_subscribe(consumer, to: producer, max_demand: 10, min_demand: 0)
      Process.sleep(700)

      # Delays 20, 40, 80, 160, 160, ... after each empty fetch: ~6 ticks.
      # A fixed 20ms interval would tick ~35 times.
      assert count(:poll_timer_executed) in 4..8
      assert Process.alive?(producer)
    end

    test "max_poll_interval resets to poll_interval once records arrive" do
      test_pid = self()
      opts = producer_opts(status: :started, poll_interval: 20, max_poll_interval: 10_000)
      {:ok, producer} = start_supervised({Producer, opts})
      {:ok, consumer} = start_supervised({KinesisClient.TestConsumer, self()})

      AppStateMock
      |> stub(:get_lease, fn _app_name, _stream_name, _shard_id, _opts ->
        %{lease_owner: opts[:lease_owner]}
      end)

      GenStage.sync_subscribe(consumer, to: producer, max_demand: 10, min_demand: 0)
      Process.sleep(400)
      assert :sys.get_state(producer).state.empty_polls > 0

      stub(KinesisMock, :get_records, fn _, _ ->
        send(test_pid, :fetched)
        records = [%{"Data" => "foo", "SequenceNumber" => "1"}]
        {:ok, %{"NextShardIterator" => "next", "MillisBehindLatest" => 0, "Records" => records}}
      end)

      # Backed off by now; fresh demand fetches immediately and finds records.
      {:ok, late} = start_supervised({KinesisClient.TestConsumer, self()}, id: :late)
      GenStage.sync_subscribe(late, to: producer, max_demand: 10, min_demand: 0)

      assert_receive {:consumer_events, _}, 1_000
      assert :sys.get_state(producer).state.empty_polls == 0
    end

    test "polls at a fixed poll_interval when max_poll_interval is not set" do
      opts = producer_opts(status: :started, poll_interval: 50)
      {:ok, producer} = start_supervised({Producer, opts})
      {:ok, consumer} = start_supervised({KinesisClient.TestConsumer, self()})

      AppStateMock
      |> stub(:get_lease, fn _app_name, _stream_name, _shard_id, _opts ->
        %{lease_owner: opts[:lease_owner]}
      end)

      GenStage.sync_subscribe(consumer, to: producer, max_demand: 10, min_demand: 0)
      flush(:poll_timer_executed)
      Process.sleep(520)

      assert count(:poll_timer_executed) in 8..11
      assert Process.alive?(producer)
    end
  end

  defp count(message, acc \\ 0) do
    receive do
      ^message -> count(message, acc + 1)
    after
      0 -> acc
    end
  end

  defp flush(message), do: count(message)

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
