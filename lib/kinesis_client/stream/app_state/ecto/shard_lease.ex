defmodule KinesisClient.Stream.AppState.Ecto.ShardLease do
  @moduledoc false
  use Ecto.Schema

  import Ecto.Changeset
  import Ecto.Query

  @type t :: %__MODULE__{}

  @fields [:shard_id, :app_name, :stream_name, :lease_owner, :lease_count, :checkpoint, :completed]

  @primary_key false
  schema "shard_lease" do
    field(:shard_id, :string, primary_key: true)
    field(:app_name, :string, primary_key: true)
    field(:stream_name, :string, primary_key: true)
    field(:lease_owner, :string)
    field(:lease_count, :integer)
    field(:checkpoint, :string)
    field(:completed, :boolean)
  end

  def changeset(shard_lease, attrs) do
    shard_lease
    |> cast(attrs, @fields)
    |> unique_constraint([:shard_id, :app_name, :stream_name], name: :shard_lease_pkey)
  end

  def query do
    from(sl in __MODULE__)
  end

  # A row is a legacy row if either name column is unset — half-populated rows
  # also break the NOT NULL migration, so this must be an OR, not an AND.
  def missing_names(query) do
    where(query, [sl], is_nil(sl.app_name) or is_nil(sl.stream_name))
  end

  # Also claims the pre-#38 owner format "worker-#{n}" (no stream prefix):
  # tables that still hold NULL-name rows predate UpdateShardLeasePrimaryKey,
  # so shard_id is still their sole primary key and they can only ever have
  # held a single stream — plain worker rows are unambiguously this stream's.
  def owned_by_stream_workers(query, stream_name) do
    pattern = escape_like(KinesisClient.Stream.worker_ref_prefix(stream_name)) <> "%"

    where(
      query,
      [sl],
      like(sl.lease_owner, ^pattern) or like(sl.lease_owner, "worker-%")
    )
  end

  def select_owner_info(query) do
    select(query, [sl], %{shard_id: sl.shard_id, lease_owner: sl.lease_owner})
  end

  def build_get_query(query, params) do
    Enum.reduce(params, query, &query_by(&1, &2))
  end

  defp query_by({:shard_id, shard_id}, query) do
    where(query, [sl], sl.shard_id == ^shard_id)
  end

  defp query_by({:app_name, app_name}, query) do
    where(query, [sl], sl.app_name == ^app_name)
  end

  defp query_by({:stream_name, stream_name}, query) do
    where(query, [sl], sl.stream_name == ^stream_name)
  end

  defp query_by({:lease_owner, lease_owner}, query) do
    where(query, [sl], sl.lease_owner == ^lease_owner)
  end

  defp query_by({:lease_count, lease_count}, query) do
    where(query, [sl], sl.lease_count == ^lease_count)
  end

  defp query_by({:completed, completed}, query) do
    where(query, [sl], sl.completed == ^completed)
  end

  # \ % and _ are LIKE metacharacters; escape them so a stream name like
  # "profile_event" cannot match "profileXevent-worker-1".
  defp escape_like(text) do
    String.replace(text, ["\\", "%", "_"], &("\\" <> &1))
  end
end
