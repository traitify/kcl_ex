defmodule KinesisClient.Stream.Shard.LoadBalanceTest do
  use ExUnit.Case, async: true

  alias KinesisClient.Stream.Shard.LoadBalance

  describe "decide/2" do
    test "balanced when there are no leases at all" do
      assert LoadBalance.decide([], "worker-1") == :balanced
    end

    test "balanced when the current worker is the only worker" do
      assert LoadBalance.decide([{"worker-1", 5}], "worker-1") == :balanced
    end

    test "balanced when leases are spread evenly" do
      counts = [{"worker-1", 3}, {"worker-2", 3}, {"worker-3", 3}]

      assert LoadBalance.decide(counts, "worker-1") == :balanced
    end

    test "steals from the most loaded worker when under target" do
      counts = [{"worker-1", 1}, {"worker-2", 4}, {"worker-3", 4}]

      assert LoadBalance.decide(counts, "worker-1") == {:steal_from, "worker-2"}
    end

    test "a worker holding no leases counts itself and steals" do
      # worker-1 holds nothing so it is absent from the grouped counts. It
      # must still count itself as a worker, see the imbalance, and steal.
      counts = [{"worker-2", 4}, {"worker-3", 4}]

      assert LoadBalance.decide(counts, "worker-1") == {:steal_from, "worker-2"}
    end

    test "does not steal when the lead is only one lease" do
      # 7 leases over 2 workers can never be more even than 4/3: stealing
      # would just flip the imbalance back and forth forever.
      counts = [{"worker-1", 3}, {"worker-2", 4}]

      assert LoadBalance.decide(counts, "worker-1") == :balanced
    end

    test "does not steal when already at target, even if another worker is over" do
      # worker-1 is at the target of 3; the deficit is worker-3's to fix.
      counts = [{"worker-1", 3}, {"worker-2", 6}, {"worker-3", 0}]

      assert LoadBalance.decide(counts, "worker-1") == :balanced
      assert LoadBalance.decide(counts, "worker-3") == {:steal_from, "worker-2"}
    end

    test "converges to an even spread as steals are applied" do
      # Simulate the {4, 4, 0} cluster rebalancing one steal at a time.
      assert LoadBalance.decide([{"w2", 4}, {"w3", 4}], "w1") == {:steal_from, "w2"}
      assert LoadBalance.decide([{"w1", 1}, {"w2", 3}, {"w3", 4}], "w1") == {:steal_from, "w3"}

      assert LoadBalance.decide([{"w1", 2}, {"w2", 3}, {"w3", 3}], "w1") == :balanced
      assert LoadBalance.decide([{"w1", 2}, {"w2", 3}, {"w3", 3}], "w2") == :balanced
      assert LoadBalance.decide([{"w1", 2}, {"w2", 3}, {"w3", 3}], "w3") == :balanced
    end
  end
end
