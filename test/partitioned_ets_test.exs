defmodule PartitionedEtsTest do
  @moduledoc """
  Tests for the `use PartitionedEts` macro form, exercised across a
  two-node `:peer` cluster.

  Validation rules and the module-form API are tested in
  `test/partitioned_ets/module_test.exs`.
  """

  use ExUnit.Case

  alias PartitionedEts.Cluster

  doctest PartitionedEts

  defmodule Table do
    @moduledoc false

    use PartitionedEts, table_opts: [:named_table, :public]

    @after_compile {Cluster, :inject}
    def hash(key, nodes) do
      send(self(), :hash_function_overloaded)
      super(key, nodes)
    end

    @spec sample_fold_fn() :: (term(), list() -> String.t())
    def sample_fold_fn do
      fn val, acc -> Enum.join(["#{inspect(val)}", acc]) end
    end

    def node_added(node) do
      if Process.whereis(:test_pid) do
        send(:test_pid, {:node_added, node})
      end
    end

    def node_removed(node) do
      if Process.whereis(:test_pid) do
        send(:test_pid, {:node_removed, node})
      end
    end
  end

  setup_all do
    Cluster.spawn([:"node1@127.0.0.1", :"node2@127.0.0.1"])

    Enum.each(Node.list(), fn node ->
      Cluster.start(node, [Table])
    end)

    :ok
  end

  setup do
    start_supervised(Table)

    on_exit(fn ->
      Table.delete_all_objects()
    end)

    :ok
  end

  @key :key
  test "insert" do
    assert Table.insert({@key, :value})
    assert [{@key, :value}] = Table.lookup(@key)
  end

  test ".insert_new" do
    assert Table.insert_new({@key, :value})
    refute Table.insert_new({@key, :value2})
    assert [{@key, :value}] = Table.lookup(@key)
  end

  test ".member" do
    assert Table.insert({@key, :value})
    assert Table.member(@key)
    refute Table.member(:none)
  end

  test ".delete" do
    assert Table.insert({@key, :value})
    assert Table.member(@key)
    assert Table.delete(@key)
    refute Table.member(@key)
  end

  test ".delete_object" do
    assert Table.insert({@key, :value})
    assert Table.member(@key)
    Table.delete_object({@key, :other_value})
    assert Table.member(@key)
    assert Table.delete_object({@key, :value})
    refute Table.member(@key)
  end

  test ".delete_all_objects" do
    assert Table.insert({@key, :value})
    assert Table.insert({@key, :value2})
    assert Table.delete_all_objects()
    refute Table.member(@key)
  end

  test ".lookup_element" do
    assert Table.insert({@key, :foo, :bar})
    assert :bar = Table.lookup_element(@key, 3)
  end

  @key :bla
  test ".match/1" do
    assert Table.insert({@key, :foo, :bar})
    assert Table.insert({:key, :foo, :bar})

    assert [[@key, :foo, :bar], [:key, :foo, :bar]] =
             Table.match({:"$1", :"$2", :"$3"})
  end

  test ".match/2" do
    assert Table.insert({@key, :foo, :bar})

    assert {[[@key, :foo, :bar]], continuation} =
             Table.match({:"$1", :"$2", :"$3"}, 1)

    assert :"$end_of_table" = Table.match(continuation)
  end

  test ".select/2" do
    assert Table.insert({@key, :foo, :bar})
    assert Table.insert({:key, :foo, :bar})

    assert [[@key, :foo, :bar], [:key, :foo, :bar]] =
             Table.select([{{:"$1", :"$2", :"$3"}, [], [:"$$"]}])
  end

  test ".select/1" do
    assert Table.insert({@key, :foo, :bar})

    assert {[[@key, :foo, :bar]], continuation} =
             Table.select([{{:"$1", :"$2", :"$3"}, [], [:"$$"]}], 1)

    assert :"$end_of_table" = Table.select(continuation)
  end

  test ".select_count/1" do
    assert Table.insert({@key, :foo, :bar})
    assert Table.insert({:key, :foo, :bar})

    assert 2 == Table.select_count([{{:"$1", :"$2", :"$3"}, [], [true]}])
  end

  test ".first/0" do
    assert :"$end_of_table" == Table.first()
    assert Table.insert({@key, :foo, :bar})
    assert Table.insert({:key, :foo, :bar})

    assert @key == Table.first()
  end

  test ".last/0" do
    assert :"$end_of_table" == Table.last()
    assert Table.insert({@key, :foo, :bar})
    assert Table.insert({:key, :foo, :bar})

    assert :key == Table.last()
  end

  test ".next/1" do
    assert Table.insert({@key, :foo, :bar})
    assert Table.insert({:key, :foo, :bar})

    assert :key == Table.next(@key)
    assert :"$end_of_table" == Table.next(:key)
  end

  test ".prev/1" do
    assert Table.insert({@key, :foo, :bar})
    assert Table.insert({:key, :foo, :bar})

    assert @key == Table.prev(:key)
    assert :"$end_of_table" == Table.prev(@key)
  end

  test ".foldl/2" do
    assert Table.insert({@key, :foo, :bar})
    assert Table.insert({:key, :foo, :bar})

    assert "{:key, :foo, :bar}{:bla, :foo, :bar}" ==
             Table.foldl(Table.sample_fold_fn(), "")

    assert_raise UndefinedFunctionError, fn ->
      Table.foldl(fn val, acc -> Enum.join(["#{inspect(val)}", acc]) end, "")
    end
  end

  test ".foldr/2" do
    assert Table.insert({@key, :foo, :bar})
    assert Table.insert({:key, :foo, :bar})

    assert "{:bla, :foo, :bar}{:key, :foo, :bar}" ==
             Table.foldr(Table.sample_fold_fn(), "")

    assert_raise UndefinedFunctionError, fn ->
      Table.foldr(fn val, acc -> Enum.join(["#{inspect(val)}", acc]) end, "")
    end
  end

  test ".match_delete/1" do
    assert Table.insert({@key, :value})
    assert Table.member(@key)
    assert Table.match_delete({:"$1", :"$2"})
    refute Table.member(@key)
  end

  test ".match_object/2" do
    assert Table.insert({@key, :value})
    assert [{@key, :value}] == Table.match_object({:_, :value})
  end

  test ".match_object/1" do
    assert Table.insert({@key, :value, :bar})

    assert {[{@key, :value, :bar}], continuation} =
             Table.match_object({:_, :value, :_}, 1)

    assert :"$end_of_table" = Table.match_object(continuation)
  end

  test ".select_delete" do
    assert Table.insert({@key, :value})
    assert Table.member(@key)
    assert Table.select_delete([{{:"$1", :"$2"}, [], [true]}])
    refute Table.member(@key)
  end

  test ".select_reverse/1" do
    assert Table.insert({@key, :foo, :bar})
    assert Table.insert({:key, :foo, :bar})

    assert [[:key, :foo, :bar], [@key, :foo, :bar]] =
             Table.select_reverse([{{:"$1", :"$2", :"$3"}, [], [:"$$"]}])
  end

  test ".select_reverse/2" do
    assert Table.insert({@key, :foo, :bar})

    assert {[[@key, :foo, :bar]], continuation} =
             Table.select_reverse([{{:"$1", :"$2", :"$3"}, [], [:"$$"]}], 1)

    assert :"$end_of_table" = Table.select_reverse(continuation)
  end

  test ".update_counter" do
    Table.insert({@key, 1})
    assert 2 == Table.update_counter(@key, {2, 1})
    assert 2 == Table.lookup_element(@key, 2)
    assert 1 == Table.update_counter(:unknown, {2, 1}, {:unknown, 0})
  end

  test ".update_element" do
    Table.insert({@key, :foo})
    assert Table.update_element(@key, {2, :bar})
    assert :bar == Table.lookup_element(@key, 2)
  end

  test ".select_replace" do
    assert Table.insert({@key, :value})
    assert 1 == Table.select_replace([{{:"$1", :_}, [], [{{:"$1", :foo}}]}])
    assert :foo == Table.lookup_element(@key, 2)
  end

  test ".take" do
    assert Table.insert({@key, :value})
    assert [{@key, :value}] = Table.lookup(@key)
    assert [{@key, :value}] == Table.take(@key)
    refute Table.member(@key)
  end

  test "using can override hash function" do
    Table.insert({@key, :value})
    assert_received(:hash_function_overloaded)
  end

  test ".tab2list" do
    assert Table.insert({@key, :foo, :bar})
    assert Table.insert({:key, :foo, :bar})
    assert [{@key, :foo, :bar}, {:key, :foo, :bar}] == Table.tab2list()
  end

  test "when adding a node node joined hook is called" do
    Process.register(self(), :test_pid)
    node = :"new_node1@127.0.0.1"

    [{:ok, pid, ^node}] =
      Cluster.spawn([node])

    Cluster.start(node, [Table])
    assert_receive {:node_added, ^node}
    :peer.stop(pid)
    assert_receive {:node_removed, ^node}
  end
end
