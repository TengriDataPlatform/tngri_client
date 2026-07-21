"""Client.sql against a DuckDB-backed websocket server."""

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
