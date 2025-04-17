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
end
