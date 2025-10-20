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

  def build_get_query(query, params) do
    Enum.reduce(params, query, &query_by(&1, &2))
  end

  defp query_by({:shard_id, shard_id}, query) do
    where(query, [sl], sl.shard_id == ^shard_id)
  end

  defp query_by({:app_name, nil}, query) do
    where(query, [sl], is_nil(sl.app_name))
  end

  defp query_by({:app_name, app_name}, query) do
    where(query, [sl], sl.app_name == ^app_name)
  end

  defp query_by({:stream_name, nil}, query) do
    where(query, [sl], is_nil(sl.stream_name))
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
end
