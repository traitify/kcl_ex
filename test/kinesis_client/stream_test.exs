defmodule KinesisClient.StreamTest do
  use KinesisClient.Case

  alias KinesisClient.Stream

  test "start_link/1 fails if :stream_name opt is missing" do
    {:error, e} = start_supervised({Stream, []})
    error = e |> elem(0) |> elem(0)
    assert error == %ArgumentError{message: "Missing required option :stream_name"}
  end

  test "start_link/1 fails if :stream_name is not a binary" do
    {:error, e} = start_supervised({Stream, [stream_name: :foo]})
    error = e |> elem(0) |> elem(0)
    assert error == %ArgumentError{message: ":stream_name must be a binary"}
  end

  test "start_link/1 fails if :app_name opt is missing" do
    stream_name = "foo_stream"

    {:error, e} = start_supervised({Stream, [stream_name: stream_name]})
    error = e |> elem(0) |> elem(0)
    assert error == %ArgumentError{message: "Missing required option :app_name"}
  end

  test "start_link/1 fails if :app_name is not a binary" do
    {:error, e} = start_supervised({Stream, [stream_name: "foo", app_name: :foo_app]})
    error = e |> elem(0) |> elem(0)
    assert error == %ArgumentError{message: ":app_name must be a binary"}
  end

  test "start_link/1 succeeds" do
    stream_name = "foo_stream"
    app_name = "foo"

    opts = [
      stream_name: stream_name,
      app_name: app_name,
      shard_consumer: KinesisClient.TestShardConsumer,
      app_state_opts: [adapter: :test]
    ]

    {:ok, pid} = start_supervised({Stream, opts})
    assert Process.alive?(pid)
  end

  test "start_link/1 fails when app_state_opts is missing" do
    stream_name = "foo_stream"
    app_name = "foo"

    opts = [
      stream_name: stream_name,
      app_name: app_name,
      shard_consumer: KinesisClient.TestShardConsumer
    ]

    assert {:error, {{%KeyError{message: message}, _}, _}} = start_supervised({Stream, opts})

    assert message ==
             "app_state_opts is a required option and needs to be included in the config. Please refer to https://github.com/traitify/kcl_ex/blob/master/README.md"
  end
end
