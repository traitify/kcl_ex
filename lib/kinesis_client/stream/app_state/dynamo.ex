defmodule KinesisClient.Stream.AppState.Dynamo do
  @moduledoc false
  @behaviour AppStateAdapter

  alias ExAws.Dynamo
  alias KinesisClient.Stream.AppState.Adapter, as: AppStateAdapter
  alias KinesisClient.Stream.AppState.ShardLease

  require Logger

  @impl AppStateAdapter
  def initialize(app_name, _opts) do
    case confirm_table_created(app_name) do
      :ok -> :ok
      {:error, {"ResourceNotFoundException", _}} -> create_table(app_name)
    end
  end

  defp create_table(app_name) do
    case app_name
         |> Dynamo.create_table("shard_id", %{shard_id: :string}, 10, 10)
         |> ExAws.request() do
      {:ok, %{}} -> confirm_table_created(app_name)
    end
  end

  defp confirm_table_created(app_name, attempts \\ 1) do
    case app_name |> Dynamo.describe_table() |> ExAws.request() do
      {:ok, %{"Table" => %{"TableStatus" => "CREATING"}}} ->
        case attempts do
          x when x <= 5 -> confirm_table_created(app_name, attempts + 1)
          _ -> raise "could not create dynamodb table!"
        end

      {:ok, _x} ->
        :ok

      {:error, _} = error ->
        error
    end
  end

  @impl AppStateAdapter
  def create_lease(app_name, shard_id, lease_owner, _opts \\ []) do
    update_opt = [condition_expression: "attribute_not_exists(shard_id)"]

    shard_lease = %ShardLease{
      shard_id: shard_id,
      lease_owner: lease_owner,
      completed: false,
      lease_count: 1
    }

    case app_name
         |> Dynamo.put_item(shard_lease, update_opt)
         |> ExAws.request()
         |> IO.inspect(label: "create_lease") do
      {:ok, _} ->
        :ok

      {:error, {"ConditionalCheckFailedException", "The conditional request failed"}} ->
        :already_exists

      output ->
        output
    end
  end

  @impl AppStateAdapter
  def get_lease(app_name, shard_id, _opts) do
    IO.inspect(app_name, label: "get_lease: app_name")
    IO.inspect(shard_id, label: "get_lease: shard_id")

    case app_name
         |> Dynamo.get_item(%{"shard_id" => shard_id})
         |> IO.inspect(label: "get_lease: Dynamo.get_item")
         |> ExAws.request()
         |> IO.inspect(label: "get_lease: ExAws.request()") do
      {:ok, %{"Item" => _} = item} -> decode_item(item)
      {:ok, _} -> :not_found
      other -> other
    end
  end

  @impl AppStateAdapter
  def renew_lease(app_name, %{shard_id: shard_id, lease_count: lease_count} = shard_lease, _opts) do
    updated_count = lease_count + 1

    update_opt = [
      condition_expression: "lease_count = :lc AND lease_owner = :lo",
      expression_attribute_values: %{
        lc: lease_count,
        lo: shard_lease.lease_owner,
        new_lease_count: updated_count
      },
      update_expression: "SET lease_count = :new_lease_count",
      return_values: "UPDATED_NEW"
    ]

    case app_name
         |> Dynamo.update_item(%{"shard_id" => shard_id}, update_opt)
         |> ExAws.request()
         |> IO.inspect(label: "renew_lease") do
      {:ok, %{"Attributes" => %{"lease_count" => _}}} -> {:ok, updated_count}
      {:error, {"ConditionalCheckFailedException", _}} -> {:error, :lease_renew_failed}
      reply -> reply
    end
  end

  @impl AppStateAdapter
  def take_lease(app_name, shard_id, new_lease_owner, lease_count, _opts) do
    updated_count = lease_count + 1

    update_opt = [
      condition_expression: "lease_count = :lc AND lease_owner <> :lo",
      expression_attribute_values: %{
        lc: lease_count,
        lo: new_lease_owner,
        new_lease_count: updated_count
      },
      update_expression: "SET lease_count = :new_lease_count, lease_owner = :lo",
      return_values: "UPDATED_NEW"
    ]

    case app_name
         |> Dynamo.update_item(%{"shard_id" => shard_id}, update_opt)
         |> ExAws.request()
         |> IO.inspect(label: "take_lease") do
      {:ok, %{"Attributes" => %{"lease_count" => _}}} -> {:ok, updated_count}
      {:error, {"ConditionalCheckFailedException", _}} -> {:error, :lease_take_failed}
      reply -> reply
    end
  end

  @impl AppStateAdapter
  def update_checkpoint(app_name, shard_id, lease_owner, checkpoint, _opts) do
    Logger.debug(
      "AppState.Dynamo updating checkpoint: [checkpoint: #{checkpoint}, shard_id: #{shard_id}]"
    )

    update_opt = [
      condition_expression: "lease_owner = :lo",
      expression_attribute_values: %{
        lo: lease_owner,
        checkpoint_num: checkpoint
      },
      update_expression: "SET checkpoint = :checkpoint_num",
      return_values: "UPDATED_NEW"
    ]

    case app_name
         |> Dynamo.update_item(%{"shard_id" => shard_id}, update_opt)
         |> ExAws.request()
         |> IO.inspect(label: "update_checkpoint") do
      {:ok, %{"Attributes" => %{"checkpoint" => %{"S" => ^checkpoint}}}} -> :ok
      {:error, {"ConditionalCheckFailedException", _}} -> {:error, :lease_owner_match}
      reply -> reply
    end
  end

  @impl AppStateAdapter
  def close_shard(app_name, shard_id, lease_owner, _opts) do
    update_opt = [
      condition_expression: "lease_owner = :lo",
      expression_attribute_values: %{
        lo: lease_owner,
        completed_v: true
      },
      update_expression: "SET completed = :completed_v",
      return_values: "UPDATED_NEW"
    ]

    case app_name
         |> Dynamo.update_item(%{"shard_id" => shard_id}, update_opt)
         |> ExAws.request()
         |> IO.inspect(label: "close_shard") do
      {:ok, %{"Attributes" => %{"completed" => %{"BOOL" => true}}}} -> :ok
      {:error, {"ConditionalCheckFailedException", _}} -> {:error, :lease_owner_match}
      reply -> reply
    end
  end

  defp decode_item(item) do
    Dynamo.decode_item(item, as: KinesisClient.Stream.AppState.ShardLease)
  end
end
