defmodule KinesisClient.Stream.AppState.Ecto.AddAppAndStreamNameColumns do
  @moduledoc false
  use Ecto.Migration

  def up do
    :shard_lease
    |> table()
    |> alter do
      add(:app_name, :string)
      add(:stream_name, :string)
    end
  end

  def version, do: 20_240_304_000_002
end
