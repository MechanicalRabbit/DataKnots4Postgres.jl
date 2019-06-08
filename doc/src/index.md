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
                CONSTRAINT patient_uk UNIQUE (id),
                CONSTRAINT patient_pk PRIMARY KEY (mrn),
                CONSTRAINT patient_mother_fk FOREIGN KEY (mother_id) REFERENCES patient (id),
                CONSTRAINT patient_father_fk FOREIGN KEY (father_id) REFERENCES patient (id)
            );

            INSERT INTO patient (id, mrn, sex, mother_id, father_id) VALUES
                (1, '99f93d58-e51c-45c0-8bee-f44c6df5a957', 'female', NULL, NULL),
                (2, '28ac2156-b841-4e0f-9416-1d8f3fccbd8e', 'male', NULL, NULL),
                (3, 'dc6194b7-2cc6-4d34-acb3-0325b0683a0e', 'male', 1, 2);
            """)

    using DataKnots, DataKnots4Postgres

    db = DataKnot(conn)
    #-> ERROR: not implemented

