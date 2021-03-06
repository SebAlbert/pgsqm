from psycopg2.sql import SQL, Composed, Identifier
import psycopg2
from typing import NamedTuple, Dict, Mapping, List, DefaultDict

class Table(NamedTuple):
    """Relation-returning query with Placeholders depicting other such queries

    Example:
    > t1 = Table(SQL("SELECT a, b FROM (VALUES (1, 100)) t(a, b)"), {})
    > t2 = Table(SQL("SELECT 5 * x.a + x.b FROM {x} WHERE x.a > 0"), {"x": t1})
    """

    sql: SQL
    source_tables: Dict[str, 'Table']

    def __hash__(self):
        return hash(id(self))

    def __eq__(self, other):
        return id(self) == id(other)


def sql_with_subqueries(table: Table, **kwargs) -> Composed:
    """Recursively build Table query with dependencies as sub-queries"""
    return table.sql.format(
        **{s: Composed([SQL("("),
                        sql_with_subqueries(t, **kwargs),
                        SQL(") AS "),
                        Identifier(s)])
           for s, t in table.source_tables.items()},
        **kwargs
    )

def sql_with_named_deps(table: Table, deps: Mapping[Table, Identifier],
                        **kwargs) -> Composed:
    """Build Table query relying on given Identifiers for dependencies"""
    return table.sql.format(
        **{s: Composed([deps[t], SQL(" AS "), Identifier(s)])
           for s, t in table.source_tables.items()},
        **kwargs
    )

def sql_with_cte(table: Table, cte_deps: List[Table], **kwargs) -> Composed:
    """Build Table query with CTEs. cte_deps must be topologically sorted"""
    names = {t: Identifier("_cte%d" % i) for i, t in enumerate(cte_deps)}
    return Composed([SQL("WITH "), SQL("\n   , ").join(Composed(
        Composed([n, SQL(" AS ("), sql_with_named_deps(t, names, **kwargs),
                  SQL(")")])
        for t, n in names.items())
        ), SQL("\n"), sql_with_named_deps(table, names, **kwargs)])

def sort_dependencies(table: Table) -> List[Table]:
    """Sort Table objects topologically according to dependencies via DFS"""
    # okay, let's not try to go strictly functional on the inside for DFS
    Pending, Done = 1, 2
    state: DefaultDict[Table, int] = DefaultDict(lambda: 0)
    todo: List[Table] = [table]
    result: List[Table] = []
    while todo:
        t: Table = todo.pop()
        deps = {d for d in t.source_tables.values()
                  if state[d] != Done}
        if not deps:
            result.append(t)
            state[t] = Done
            continue
        todo.append(t)
        for d in deps:
            assert state[d] is not Pending, "Circular Table dependency"
            state[d] = Pending
        todo += deps
    return result

hund = Table(SQL("SELECT {c}, b FROM (VALUES (1, 100), (2, 200)) t({c}, b)"), {})
thou = Table(SQL("SELECT {c}, b FROM (VALUES (1, 1000)) t({c}, b)"), {})

combine = Table(SQL("SELECT h.{c}, h.b, t.b FROM {h} LEFT JOIN {t} USING (a) "
                    "LEFT JOIN {q} USING (a)"),
                {"h": hund, "t": thou, "q": hund})

conn = psycopg2.connect("")

args = {"c": Identifier("a")}

print(sql_with_subqueries(combine, **args).as_string(conn))
print(sql_with_cte(combine, sort_dependencies(combine)[:-1], **args)
        .as_string(conn))
