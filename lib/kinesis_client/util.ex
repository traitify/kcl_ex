defmodule KinesisClient.Util do
  @moduledoc false
  def optional_kw(keywords, _name, nil) do
    keywords
  end

  def optional_kw(keywords, name, value) do
    Keyword.put(keywords, name, value)
  end

  def register_name(module, app_name, stream_name, addtnl \\ []) do
    Module.concat([module, app_name, stream_name] ++ addtnl)
  end

  @doc """
  Sends `message` to the pid in the state's `:notify` field, if one is set.

  Used by the lease and rebalancer processes to expose lifecycle events to
  tests.
  """
  def notify(_message, %{notify: nil}) do
    :ok
  end

  def notify(message, %{notify: pid}) do
    send(pid, message)
    :ok
  end
end
