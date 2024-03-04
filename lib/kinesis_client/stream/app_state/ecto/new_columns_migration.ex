defmodule KinesisClient.Stream.AppState.Ecto.NewColumnsMigration do
  @moduledoc false
  use Ecto.Migration

  def up do
    :shard_lease
    |> table()
    |> alter do
      add(:app_name, :string)
      add(:stream_name, :string)
    end

    flush()

    execute("UPDATE shard_lease SET app_name = 'assessment_service'")

    stream_name =
      if Application.get_env(:assessment_service, :env) == :prod do
        System.fetch_env!("PROFILE_STREAM_NAME")
      else
        "profile-event-stag"
      end

    execute("UPDATE shard_lease SET stream_name = '#{stream_name}'")
  end
end
