"""Client.sql against a DuckDB-backed websocket server."""

import pickle
import threading

import pandas as pd
import pytest


def test_sql_select_types(sql_env):
    client, _ = sql_env
    # 1.5 cast to DOUBLE: a bare 1.5 is DECIMAL, which the wire serializes as a
    # string (as it does against the real gateway).
    out = client.sql("SELECT 1 AS i, 'x' AS s, CAST(1.5 AS DOUBLE) AS f")
    assert out.shape == (1, 3)
    assert out["i"].iloc[0] == 1
    assert out["s"].iloc[0] == "x"
    assert out["f"].iloc[0] == 1.5


def test_sql_multiple_rows(sql_env):
    client, _ = sql_env
    out = client.sql("SELECT * FROM (VALUES (1), (2), (3)) AS t(n) ORDER BY n")
    assert out["n"].tolist() == [1, 2, 3]


def test_sql_reads_seeded_table(sql_env):
    client, con = sql_env
    con.execute("CREATE TABLE t AS SELECT * FROM (VALUES (10, 'ten')) AS v(id, name)")
    out = client.sql("SELECT id, name FROM t")
    assert out["id"].iloc[0] == 10
    assert out["name"].iloc[0] == "ten"


def test_sql_error_is_raised(sql_env):
    client, _ = sql_env
    with pytest.raises(RuntimeError, match="Error while executing"):
        client.sql("SELECT * FROM this_table_does_not_exist")


def test_sql_returns_the_first_batch_as_a_dataframe(sql_env):
    client, con = sql_env
    con.execute("CREATE TABLE t AS SELECT range AS n FROM range(25)")
    out = client.sql("SELECT n FROM t ORDER BY n", batch_rows=10)
    assert isinstance(out, pd.DataFrame)
    assert out["n"].tolist() == list(range(10))
    assert type(out.head(3)) is pd.DataFrame


def test_batches_stream_every_row(sql_env):
    client, con = sql_env
    con.execute("CREATE TABLE t AS SELECT range AS n FROM range(25)")
    batches = list(client.sql("SELECT n FROM t ORDER BY n", batch_rows=10).batches())
    assert [len(batch) for batch in batches] == [10, 10, 5]
    assert pd.concat(batches)["n"].tolist() == list(range(25))


def test_batches_end_without_an_empty_tail(sql_env):
    client, con = sql_env
    con.execute("CREATE TABLE t AS SELECT range AS n FROM range(20)")
    batches = list(client.sql("SELECT n FROM t", batch_rows=10).batches())
    assert [len(batch) for batch in batches] == [10, 10]


def test_an_empty_result_is_one_empty_batch_with_columns(sql_env):
    client, _ = sql_env
    batches = list(client.sql("SELECT 1 AS n WHERE false", batch_rows=10).batches())
    assert [len(batch) for batch in batches] == [0]
    assert batches[0].columns.tolist() == ["n"]


def test_batches_continue_the_query_instead_of_running_it_again(sql_env):
    client, con = sql_env
    con.execute("CREATE SEQUENCE s")
    result = client.sql("SELECT nextval('s') AS n FROM range(25)", batch_rows=10)
    assert pd.concat(result.batches())["n"].tolist() == list(range(1, 26))


def test_batches_can_be_read_only_once(sql_env):
    client, _ = sql_env
    result = client.sql("SELECT 1 AS n", batch_rows=10)
    list(result.batches())
    with pytest.raises(RuntimeError, match="result set is already closed"):
        list(result.batches())


def test_closing_a_result_ends_its_batches(sql_env):
    client, con = sql_env
    con.execute("CREATE TABLE t AS SELECT range AS n FROM range(25)")
    with client.sql("SELECT n FROM t ORDER BY n", batch_rows=10) as result:
        batches = result.batches()
        assert next(batches)["n"].tolist() == list(range(10))
    assert list(batches) == []
    with pytest.raises(RuntimeError, match="result set is already closed"):
        list(result.batches())


def test_the_next_query_closes_an_unread_result(sql_env):
    client, _ = sql_env
    unread = client.sql("SELECT 1 AS n", batch_rows=10)
    client.sql("SELECT 2")
    with pytest.raises(RuntimeError, match="result set is already closed"):
        list(unread.batches())


def test_the_next_query_leaves_a_result_being_read(sql_env):
    client, con = sql_env
    con.execute("CREATE TABLE t AS SELECT range AS n FROM range(25)")
    read = []
    for batch in client.sql("SELECT n FROM t ORDER BY n", batch_rows=10).batches():
        read.append(batch)
        client.sql("SELECT 1")
    assert pd.concat(read)["n"].tolist() == list(range(25))


def test_a_query_on_another_thread_leaves_this_threads_result(sql_env):
    client, con = sql_env
    con.execute("CREATE TABLE t AS SELECT range AS n FROM range(25)")
    result = client.sql("SELECT n FROM t ORDER BY n", batch_rows=10)
    other = threading.Thread(target=client.sql, args=("SELECT 1",))
    other.start()
    other.join()
    assert pd.concat(result.batches())["n"].tolist() == list(range(25))


def test_an_unpickled_result_has_no_batches(sql_env):
    client, _ = sql_env
    result = pickle.loads(pickle.dumps(client.sql("SELECT 1 AS n", batch_rows=10)))
    assert result["n"].tolist() == [1]
    with pytest.raises(RuntimeError, match="result set is already closed"):
        list(result.batches())
