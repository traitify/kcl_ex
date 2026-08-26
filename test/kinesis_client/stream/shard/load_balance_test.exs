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
      # The steal count is half the gap to the victim (1), not the full
      # deficit of 2 — taking 2 would leave the victim below worker-1.
      counts = [{"worker-1", 1}, {"worker-2", 4}, {"worker-3", 4}]

      assert LoadBalance.decide(counts, "worker-1") == {:steal_from, "worker-2", 1}
    end

    test "a worker holding no leases counts itself and steals" do
      # worker-1 holds nothing so it is absent from the grouped counts. It
      # must still count itself as a worker, see the imbalance, and steal.
      counts = [{"worker-2", 4}, {"worker-3", 4}]

      assert LoadBalance.decide(counts, "worker-1") == {:steal_from, "worker-2", 2}
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
      assert LoadBalance.decide(counts, "worker-3") == {:steal_from, "worker-2", 3}
    end

    test "stealing the full steal_count lands on a balanced spread" do
      # {A: 4, B: 0}: taking more than 2 would overshoot to {1, 3} and
      # oscillate forever. Taking exactly the steal count settles it.
      assert LoadBalance.decide([{"worker-a", 4}], "worker-b") == {:steal_from, "worker-a", 2}

      settled = [{"worker-a", 2}, {"worker-b", 2}]
      assert LoadBalance.decide(settled, "worker-a") == :balanced
      assert LoadBalance.decide(settled, "worker-b") == :balanced
    end

    test "steal_count never flips the pairwise gap ({2, 4, 4} live-lock regression)" do
      # The deficit here is 2 (target 4), but stealing 2 from worker-b flips
      # {2, 4} to {4, 2} and the pair trades the same leases forever. Half
      # the gap (1) moves it to {3, 3, 4}, which is balanced.
      counts = [{"worker-a", 2}, {"worker-b", 4}, {"worker-c", 4}]

      assert LoadBalance.decide(counts, "worker-a") == {:steal_from, "worker-b", 1}

      settled = [{"worker-a", 3}, {"worker-b", 3}, {"worker-c", 4}]
      assert LoadBalance.decide(settled, "worker-a") == :balanced
      assert LoadBalance.decide(settled, "worker-b") == :balanced
      assert LoadBalance.decide(settled, "worker-c") == :balanced
    end

    test "converges for {5, 5, 0} without flipping" do
      assert LoadBalance.decide([{"w1", 5}, {"w2", 5}], "w3") == {:steal_from, "w1", 2}
      assert LoadBalance.decide([{"w1", 3}, {"w2", 5}, {"w3", 2}], "w3") == {:steal_from, "w2", 1}

      settled = [{"w1", 3}, {"w2", 4}, {"w3", 3}]
      assert LoadBalance.decide(settled, "w1") == :balanced
      assert LoadBalance.decide(settled, "w2") == :balanced
      assert LoadBalance.decide(settled, "w3") == :balanced
    end

    test "converges to an even spread as steals are applied" do
      # Simulate the {4, 4, 0} cluster rebalancing round by round.
      assert LoadBalance.decide([{"w2", 4}, {"w3", 4}], "w1") == {:steal_from, "w2", 2}
      assert LoadBalance.decide([{"w1", 2}, {"w2", 2}, {"w3", 4}], "w1") == {:steal_from, "w3", 1}

      assert LoadBalance.decide([{"w1", 3}, {"w2", 2}, {"w3", 3}], "w1") == :balanced
      assert LoadBalance.decide([{"w1", 3}, {"w2", 2}, {"w3", 3}], "w2") == :balanced
      assert LoadBalance.decide([{"w1", 3}, {"w2", 2}, {"w3", 3}], "w3") == :balanced
    end
  end
end
