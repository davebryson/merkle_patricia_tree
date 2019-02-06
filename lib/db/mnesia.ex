defmodule MerklePatriciaTree.DB.Mnesia do
  @moduledoc """
  Mnesia backed storage for the Tree.
  """
  alias :mnesia, as: Mnesia
  alias MerklePatriciaTree.DB
  alias MerklePatriciaTree.Trie

  @behaviour MerklePatriciaTree.DB

  @doc """
  Initialize the database.
  ## Example
    db = MerklePatriciaTree.DB.Mnesia.init(:your_table_name) 
    trie = MerklePatriciaTree.Trie.new(db)
  """
  @spec init(DB.db_name()) :: DB.db()
  def init(db_name) when is_atom(db_name) do
    # create schema
    {:ok, result} = create_db_schema()
    # start mnesia
    :ok = Application.ensure_started(:mnesia)
    # create table if not exists
    {:atomic, :ok} = create_table(result, db_name)
    {__MODULE__, db_name}
  end

  @spec get(DB.db_ref(), Trie.key()) :: {:ok, DB.value()} | :not_found
  def get(db_ref, key) do
    {:atomic, result} =
      Mnesia.transaction(fn ->
        Mnesia.read({db_ref, key})
      end)

    process_result(db_ref, result)
  end

  defp process_result(db_ref, []), do: :not_found
  defp process_result(db_ref, [{db_ref, _k, value}]), do: {:ok, value}

  @spec put!(DB.db_ref(), Trie.key(), DB.value()) :: :ok
  def put!(db_ref, key, value) do
    result =
      Mnesia.transaction(fn ->
        Mnesia.write({db_ref, key, value})
      end)

    case result do
      {:aborted, _} -> :aborted
      _ -> :ok
    end
  end

  defp create_db_schema() do
    case Mnesia.create_schema([node()]) do
      {:error, {_n, {:already_exists, _n}}} ->
        {:ok, :exists}

      {:error, reason} ->
        :error

      _ ->
        {:ok, :create}
    end
  end

  # returns {:atomic, :ok} | {:aborted, Reason}
  defp create_table(:exists, _), do: {:atomic, :ok}
  defp create_table(:create, db_name) do
    Mnesia.create_table(db_name, [
      {:disc_only_copies, [node()]},
      {:attributes, [:key, :value]}
    ])
  end
end
