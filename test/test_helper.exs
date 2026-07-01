Mox.defmock(KinesisClient.KinesisMock, for: KinesisClient.Kinesis.Adapter)
Mox.defmock(KinesisClient.Stream.AppStateMock, for: KinesisClient.Stream.AppState.Adapter)

Application.put_env(:ex_aws, :dynamodb,
  scheme: "http://",
  host: "localhost",
  port: "4566",
  region: "us-east-1"
)

Application.put_env(:ex_aws, :kinesis,
  scheme: "http://",
  host: "localhost",
  port: "4566",
  region: "us-east-1"
)

Logger.configure(level: :info)

# Integration tests (e.g. DynamoTest) require a running DynamoDB and are
# excluded by default. Run them with `mix test --include integration`.
ExUnit.configure(exclude: [:integration])

ExUnit.start()
