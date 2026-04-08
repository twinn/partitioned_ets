defmodule PartitionedEts.ModuleTest do
  @moduledoc """
  Module-form (`PartitionedEts.insert(table, ...)`) API tests.

  These run on a single node — no `:peer` cluster — so they exercise
  the routing layer with a degenerate one-node membership and stay
  fast. The cross-node behaviour of the same API is covered by
  `PartitionedEtsTest` via the macro form.
  """

  use ExUnit.Case, async: false

  describe "validation" do
    test "requires :named_table" do
      assert_raise RuntimeError,
                   ~r/only supports `:named_table`/,
                   fn ->
                     PartitionedEts.start_link(name: :v_named, table_opts: [:public])
                   end
    end

    test "requires :public" do
      assert_raise RuntimeError,
                   ~r/only supports `:public`/,
                   fn ->
                     PartitionedEts.start_link(name: :v_public, table_opts: [:named_table])
                   end
    end

    test "rejects :private" do
      assert_raise RuntimeError,
                   ~r/does not support `:private`/,
                   fn ->
                     PartitionedEts.start_link(
                       name: :v_priv,
                       table_opts: [:private, :named_table, :public]
                     )
                   end
    end

    test "rejects :protected" do
      assert_raise RuntimeError,
                   ~r/does not support `:protected`/,
                   fn ->
                     PartitionedEts.start_link(
                       name: :v_prot,
                       table_opts: [:protected, :named_table, :public]
                     )
                   end
    end

    test "rejects keypos other than 1" do
      assert_raise RuntimeError,
                   ~r/only supports `keypos: 1`/,
                   fn ->
                     PartitionedEts.start_link(
                       name: :v_kp,
                       table_opts: [:named_table, :public, {:keypos, 2}]
                     )
                   end
    end
  end

  describe "module-form API" do
    setup do
      table = :"module_form_#{:erlang.unique_integer([:positive])}"
      pid = start_supervised!({PartitionedEts, name: table, table_opts: [:named_table, :public]})
      {:ok, table: table, pid: pid}
    end

    test "insert/lookup", %{table: t} do
      assert PartitionedEts.insert(t, {:key, :value})
      assert [{:key, :value}] == PartitionedEts.lookup(t, :key)
    end

    test "insert_new", %{table: t} do
      assert PartitionedEts.insert_new(t, {:k, 1})
      refute PartitionedEts.insert_new(t, {:k, 2})
      assert [{:k, 1}] == PartitionedEts.lookup(t, :k)
    end

    test "member/delete", %{table: t} do
      PartitionedEts.insert(t, {:k, :v})
      assert PartitionedEts.member(t, :k)
      PartitionedEts.delete(t, :k)
      refute PartitionedEts.member(t, :k)
    end

    test "delete_object", %{table: t} do
      PartitionedEts.insert(t, {:k, :v})
      PartitionedEts.delete_object(t, {:k, :other})
      assert PartitionedEts.member(t, :k)
      PartitionedEts.delete_object(t, {:k, :v})
      refute PartitionedEts.member(t, :k)
    end

    test "delete_all_objects", %{table: t} do
      PartitionedEts.insert(t, {:a, 1})
      PartitionedEts.insert(t, {:b, 2})
      PartitionedEts.delete_all_objects(t)
      refute PartitionedEts.member(t, :a)
      refute PartitionedEts.member(t, :b)
    end

    test "lookup_element", %{table: t} do
      PartitionedEts.insert(t, {:k, :foo, :bar})
      assert :bar == PartitionedEts.lookup_element(t, :k, 3)
    end

    test "match across all nodes", %{table: t} do
      PartitionedEts.insert(t, {:a, 1, 2})
      PartitionedEts.insert(t, {:b, 3, 4})
      results = PartitionedEts.match(t, {:"$1", :"$2", :"$3"})
      assert length(results) == 2
    end

    test "select", %{table: t} do
      PartitionedEts.insert(t, {:a, 1})
      PartitionedEts.insert(t, {:b, 2})
      results = PartitionedEts.select(t, [{{:"$1", :"$2"}, [], [:"$$"]}])
      assert length(results) == 2
    end

    test "select_count sums across nodes", %{table: t} do
      PartitionedEts.insert(t, {:a, 1})
      PartitionedEts.insert(t, {:b, 2})
      assert 2 == PartitionedEts.select_count(t, [{{:"$1", :"$2"}, [], [true]}])
    end

    test "tab2list", %{table: t} do
      PartitionedEts.insert(t, {:a, 1})
      PartitionedEts.insert(t, {:b, 2})
      list = PartitionedEts.tab2list(t)
      assert length(list) == 2
    end

    test "update_counter", %{table: t} do
      PartitionedEts.insert(t, {:k, 1})
      assert 2 == PartitionedEts.update_counter(t, :k, {2, 1})
      assert 1 == PartitionedEts.update_counter(t, :missing, {2, 1}, {:missing, 0})
    end

    test "update_element", %{table: t} do
      PartitionedEts.insert(t, {:k, :foo})
      assert PartitionedEts.update_element(t, :k, {2, :bar})
      assert :bar == PartitionedEts.lookup_element(t, :k, 2)
    end

    test "take", %{table: t} do
      PartitionedEts.insert(t, {:k, :v})
      assert [{:k, :v}] == PartitionedEts.take(t, :k)
      refute PartitionedEts.member(t, :k)
    end

    test "match_delete", %{table: t} do
      PartitionedEts.insert(t, {:a, 1})
      PartitionedEts.insert(t, {:b, 2})
      PartitionedEts.match_delete(t, {:"$1", :"$2"})
      refute PartitionedEts.member(t, :a)
      refute PartitionedEts.member(t, :b)
    end

    test "select_delete", %{table: t} do
      PartitionedEts.insert(t, {:a, 1})
      PartitionedEts.select_delete(t, [{{:"$1", :"$2"}, [], [true]}])
      refute PartitionedEts.member(t, :a)
    end

    test "select_replace", %{table: t} do
      PartitionedEts.insert(t, {:k, :v})
      assert 1 == PartitionedEts.select_replace(t, [{{:"$1", :_}, [], [{{:"$1", :foo}}]}])
      assert :foo == PartitionedEts.lookup_element(t, :k, 2)
    end

    test "foldl", %{table: t} do
      PartitionedEts.insert(t, {:a, 1})
      PartitionedEts.insert(t, {:b, 2})
      sum = PartitionedEts.foldl(t, fn {_, n}, acc -> n + acc end, 0)
      assert sum == 3
    end
  end

  describe "callbacks module" do
    defmodule HashCallbacks do
      @moduledoc false
      def hash(key, nodes) do
        send(:cb_test, {:hashed, key})
        key |> :erlang.phash2(length(nodes)) |> then(&Enum.at(nodes, &1))
      end
    end

    test "user-supplied :callbacks module's hash/2 is invoked on routing" do
      Process.register(self(), :cb_test)
      table = :"cb_#{:erlang.unique_integer([:positive])}"

      start_supervised!({PartitionedEts, name: table, table_opts: [:named_table, :public], callbacks: HashCallbacks})

      PartitionedEts.insert(table, {:hello, :world})
      assert_received {:hashed, :hello}
    end
  end
end
