# Exploring Pipelines

This document walks though how a DataKnots query is translated into a
pipeline and then into SQL. First, we'll create a set of sample data
with `patient` table.

    include("sampledata.jl")

    using DataKnots, DataKnots4Postgres
    using DataKnots: assemble

    db = DataKnot(conn);

    @query db patient
    #=>
      │ patient │
    ──┼─────────┼
    1 │ 1       │
    2 │ 2       │
    3 │ 3       │
    4 │ 4       │
    =#

    assemble(db, @query patient)
    #=>
    chain_of(with_elements(load_postgres_table(("public", "patient"),
                                               ["id"],
                                               [Int32])),
             flatten())
    =#

    @query db patient{mrn, sex}
    #=>
      │ patient          │
      │ mrn       sex    │
    ──┼──────────────────┼
    1 │ 99f93d58  female │
    2 │ 28ac2156  male   │
    3 │ dc6194b7  male   │
    4 │ 3126ce41  female │
    =#

    assemble(db, @query patient{mrn, sex})
    #=>
    chain_of(
        with_elements(
            load_postgres_table(("public", "patient"), ["id"], [Int32])),
        flatten(),
        with_elements(
            tuple_of(
                :mrn => chain_of(
                            load_postgres_table(("public", "patient"),
                                                ["mrn"],
                                                [String],
                                                ["id"]),
                            block_cardinality(x1to1),
                            with_elements(chain_of(output(), column(1)))),
                :sex => chain_of(load_postgres_table(("public", "patient"),
                                                     ["sex"],
                                                     [String],
                                                     ["id"]),
                                 block_cardinality(x1to1),
                                 with_elements(chain_of(output(),
                                                        column(1)))))))
    =#
