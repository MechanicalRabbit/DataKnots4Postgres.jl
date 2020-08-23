# DataKnots4Postgres.jl

    using LibPQ

    conn = LibPQ.Connection("")

    execute(conn, "BEGIN")

    execute(conn,
            """
            CREATE TYPE patient_sex_enum AS ENUM ('male', 'female', 'other', 'unknown');

            CREATE TABLE patient (
                id int4 NOT NULL,
                mrn text NOT NULL,
                sex patient_sex_enum NOT NULL DEFAULT 'unknown',
                mother_id int4,
                father_id int4,
                CONSTRAINT patient_pk PRIMARY KEY (id),
                CONSTRAINT patient_mrn_uk UNIQUE (mrn),
                CONSTRAINT patient_mother_fk FOREIGN KEY (mother_id) REFERENCES patient (id),
                CONSTRAINT patient_father_fk FOREIGN KEY (father_id) REFERENCES patient (id)
            );

            INSERT INTO patient (id, mrn, sex, mother_id, father_id) VALUES
                (1, '99f93d58', 'female', NULL, NULL),
                (2, '28ac2156', 'male', NULL, NULL),
                (3, 'dc6194b7', 'male', 1, 2),
                (4, '3126ce41', 'female', 1, 2);
            """)

    using DataKnots, DataKnots4Postgres

    db = DataKnot(conn)
    #=>
    ┼─────────── … ──┼
    │ DATABASE " … " │
    =#

    @query db patient
    #=>
      │ patient │
    ──┼─────────┼
    1 │ 1       │
    2 │ 2       │
    3 │ 3       │
    4 │ 4       │
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

    @query db patient.filter(sex=="female")
    #=>
      │ patient │
    ──┼─────────┼
    1 │ 1       │
    2 │ 4       │
    =#

    @query db count(patient)
    #=>
    ┼───┼
    │ 4 │
    =#

    @query db patient.group(sex){sex, size => count(patient)}
    #=>
      │ sex     size │
    ──┼──────────────┼
    1 │ female     2 │
    2 │ male       2 │
    =#

    @query db begin
        patient
        keep(p => it)
        {mrn, sex}
        join(mother => patient.filter(id == p.mother_id).is0to1())
        join(father => patient.filter(id == p.father_id).is0to1())
        {mrn, sex, mother => mother.mrn, father => father.mrn}
    end
    #=>
      │ patient                              │
      │ mrn       sex     mother    father   │
    ──┼──────────────────────────────────────┼
    1 │ 99f93d58  female                     │
    2 │ 28ac2156  male                       │
    3 │ dc6194b7  male    99f93d58  28ac2156 │
    4 │ 3126ce41  female  99f93d58  28ac2156 │
    =#

    @query db begin
        patient
        {
            mrn,
            mother => patient_mother_fk.mrn,
            father => patient_father_fk.mrn,
            maternal_children => patient_via_patient_mother_fk.mrn,
            paternal_children => patient_via_patient_father_fk.mrn,
        }
    end
    #=>
      │ patient                                                           │
      │ mrn       mother    father    maternal_children  paternal_childre…│
    ──┼───────────────────────────────────────────────────────────────────┼
    1 │ 99f93d58                      dc6194b7; 3126ce4…                  │
    2 │ 28ac2156                                         dc6194b7; 3126ce…│
    3 │ dc6194b7  99f93d58  28ac2156                                      │
    4 │ 3126ce41  99f93d58  28ac2156                                      │
    =#

