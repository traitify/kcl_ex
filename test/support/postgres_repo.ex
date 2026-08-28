defmodule KinesisClient.Test.PostgresRepo do
  @moduledoc """
  A real Postgres repo for integration tests (`mix test --include integration`).

  Connection is taken from the standard PG* environment variables, falling back
  to a local default (localhost/postgres/postgres, database kcl_ex_test).
  """
  use Ecto.Repo,
    otp_app: :kinesis_client,
    adapter: Ecto.Adapters.Postgres

  def default_config do
    [
      hostname: System.get_env("PGHOST", "localhost"),
      port: String.to_integer(System.get_env("PGPORT", "5432")),
      username: System.get_env("PGUSER", "postgres"),
      password: System.get_env("PGPASSWORD", "postgres"),
      database: System.get_env("PGDATABASE", "kcl_ex_test"),
      pool_size: 2
    ]
  end
end
