# KCL

Implements a native Elixir implementation of Amazon's Kinesis Client Library
(KCL). The KCL is a Java library that uses a DynamoDB table to keep track of
how far an app has processed a Kinesis stream and to correctly handle shard
splits and merges.

By using this library, you get the above functionality without the need to
deploy a the KCL Multilang Daemon.


## Install
Add this to your dependencies
```
    {:kinesis_client, "~> 0.1.0"},
```
and run `mix deps.get`

## Usage

Stream processing and acknowledgement is handled in a Broadway pipeline. Here's
a basic configuration:

```elixir
opts = [
  stream_name: "kcl-ex-test-stream",
  app_name: "my-test-app",
  shard_consumer: MyShardConsumer,
  app_state_opts: [
    adapter: :ecto | :dynamo | :migrate,
    # the repo option is required if the adapter is :repo
    repo: AssessmentService.Repo,
    # below options are required if the adapter is :migrate
    migration: [from: :dynamo, to: :ecto],
    app_name: app_name,
    stream_name: stream_name
  ],
  # optional, how often (ms) a held lease is renewed. Default: 30_000
  lease_renew_interval: 30_000,
  # optional, how long (ms) a lease can go unrenewed before another worker
  # may take it. Must be greater than lease_renew_interval. Default: 45_001
  lease_expiry: 45_001,
  # optional, how often (ms) this worker checks whether leases are spread
  # evenly across workers and steals one from an overloaded worker if not.
  # Default: 6_000
  rebalance_interval: 6_000,
  # optional, the maximum number of leases to steal per rebalance check.
  # Default: 1
  max_leases_to_steal: 1,
  # optional poll_interval for getting records from kinesis
  poll_interval: 500,
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

KinesisClient.Stream.start_link(opts)
```

`MyShardConsumer` needs to implement the `Broadway` behaviour. You will want to
start the `KinesisClient.Stream` in your application's supervision tree.

## partition_by option

If you want to include the partition_by option to the Broadway pipeline 
then you need to implement a partition_by/1 function in the consumer `MyShardConsumer`.


## Things to keep in mind...

If you're concerned with processing every message in your Kinesis Stream
successfully, you'll likely want to keep processor and batch concurrency set to `1`.
This is because you can only process a Kinesis stream by checkpointing where
you're at, as opposed to ack-ing individual messages like you can with SQS.
Increase the number of shards if you want to increase processing throughput.

If increasing the number of shards is not possible or desirable, I would
recommend fanning out in the `handle_batch/4` callback of your shard consumer. 
Configuring dead letter queues and partitioning are dependent on your
application's requirements and the structure of your data.


## Development

the tests by default require
[localstack](https://github.com/localstack/localstack) to be installed and
running. How to do that is outside the scope of this readme, but here's how I'm
doing it currently:
```
SERVICES=kinesis,dynamodb localstack start --host
```

## Load balancing

Each worker runs a single rebalancer process that periodically (every
`rebalance_interval`, jittered +/- 25%) compares how many incomplete leases
each worker holds. When this worker is below the target load
(`ceil(total_shards / total_workers)`) and another worker leads it by more
than one lease, it steals from the worker holding the most — up to its lease
deficit per check, capped at `max_leases_to_steal` (default 1) — so workers
converge on an even spread without overshooting. Workers that crash stop renewing their leases,
and after `lease_expiry` the remaining workers take those leases over.

## TODO
- [ ] Test shard merges and splits more thoroughly

