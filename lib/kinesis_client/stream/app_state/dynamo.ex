defmodule KinesisClient.Stream.AppState.Dynamo do
  @moduledoc false

  @behaviour KinesisClient.Stream.AppState.Adapter

  alias ExAws.Dynamo
  alias KinesisClient.Stream.AppState.ShardLease

  require Logger

  @impl true
  def initialize(app_name, _opts) do
    case confirm_table_created(app_name) do
      :ok -> :ok
      {:error, {"ResourceNotFoundException", _}} -> create_table(app_name)
    end
  end

  defp create_table(app_name) do
    app_name
    |> send_create_table_request()
    |> confirm_table_created()
  end

  defp send_create_table_request(app_name) do
    app_name
    |> Dynamo.create_table("shard_id", [{:shard_id, :string}], 10, 10)
    |> ExAws.request()

    app_name
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

  @impl true
  def get_leases_by_worker(_app_name, _stream_name, _lease_owner, _opts) do
    raise BadFunctionError,
      message:
        "get_leases_by_worker/4 is not currently implemented for DynamoDB. Please implement the callback if you want to use DynamoDB."
  end

  @impl true
  def create_lease(app_name, _stream_name, shard_id, lease_owner, _opts \\ []) do
    update_opt = [condition_expression: "attribute_not_exists(shard_id)"]

    shard_lease = %ShardLease{
      shard_id: shard_id,
      lease_owner: lease_owner,
      completed: false,
      lease_count: 1
    }

    case app_name
         |> Dynamo.put_item(shard_lease, update_opt)
         |> ExAws.request() do
      {:ok, _} ->
        :ok

      {:error, {"ConditionalCheckFailedException", "The conditional request failed"}} ->
        :already_exists

      output ->
        output
    end
  end

  @impl true
  def get_lease(app_name, _stream_name, shard_id, _opts) do
    Logger.debug("(get_lease).app_name: #{app_name}")
    Logger.debug("(get_lease).shard_id: #{shard_id}")

    case app_name
         |> Dynamo.get_item(%{"shard_id" => shard_id})
         |> ExAws.request() do
      {:ok, %{"Item" => _} = item} -> decode_item(item)
      {:ok, _} -> :not_found
      other -> other
    end
  end

  @impl true
  def renew_lease(
        app_name,
        _stream_name,
        %{shard_id: shard_id, lease_count: lease_count} = shard_lease,
        _opts
      ) do
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
         |> ExAws.request() do
      {:ok, %{"Attributes" => %{"lease_count" => _}}} -> {:ok, updated_count}
      {:error, {"ConditionalCheckFailedException", _}} -> {:error, :lease_renew_failed}
      reply -> reply
    end
  end

  @impl true
  def take_lease(app_name, _stream_name, shard_id, new_lease_owner, lease_count, _opts) do
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
         |> ExAws.request() do
      {:ok, %{"Attributes" => %{"lease_count" => _}}} -> {:ok, updated_count}
      {:error, {"ConditionalCheckFailedException", _}} -> {:error, :lease_take_failed}
      reply -> reply
    end
  end

  @impl true
  def update_checkpoint(app_name, _stream_name, shard_id, lease_owner, checkpoint, _opts) do
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
         |> ExAws.request() do
      {:ok, %{"Attributes" => %{"checkpoint" => %{"S" => ^checkpoint}}}} -> :ok
      {:error, {"ConditionalCheckFailedException", _}} -> {:error, :lease_owner_match}
      reply -> reply
    end
  end

  @impl true
  def close_shard(app_name, _stream_name, shard_id, lease_owner, _opts) do
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
         |> ExAws.request() do
      {:ok, %{"Attributes" => %{"completed" => %{"BOOL" => true}}}} -> :ok
      {:error, {"ConditionalCheckFailedException", _}} -> {:error, :lease_owner_match}
      reply -> reply
    end
  end

  @impl true
  def all_incomplete_leases(_app_name, _stream_name, _opts) do
    Logger.error(
      "all_incomplete_leases/3 is not currently implemented for DynamoDB. Please implement the callback if you want to use DynamoDB."
    )

    []
  end

  @impl true
  def lease_owner_with_most_leases(_app_name, _stream_name, _opts) do
    Logger.error(
      "lease_owner_with_most_leases/3 is not currently implemented for DynamoDB. Please implement the callback if you want to use DynamoDB."
    )

    []
  end

  @impl true
  def total_incomplete_lease_counts_by_worker(_app_name, _stream_name, _opts) do
    Logger.error(
      "total_incomplete_lease_counts_by_worker/3 is not currently implemented for DynamoDB. Please implement the callback if you want to use DynamoDB."
    )

    []
  end

  defp decode_item(item) do
    Dynamo.decode_item(item, as: KinesisClient.Stream.AppState.ShardLease)
  end
end
