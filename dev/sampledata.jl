using LibPQ
conn = LibPQ.Connection("")
execute(conn, "BEGIN")
execute(conn, """
CREATE TYPE patient_sex_enum AS 
    ENUM ('male', 'female', 'other', 'unknown');
CREATE TABLE patient (
    id int4 NOT NULL,
    mrn text NOT NULL,
    sex patient_sex_enum NOT NULL DEFAULT 'unknown',
    mother_id int4,
    father_id int4,
    CONSTRAINT patient_pk PRIMARY KEY (id),
    CONSTRAINT patient_mrn_uk UNIQUE (mrn),
    CONSTRAINT patient_mother_fk FOREIGN KEY (mother_id) 
                                 REFERENCES patient (id),
    CONSTRAINT patient_father_fk FOREIGN KEY (father_id) 
                                 REFERENCES patient (id)
);
INSERT INTO patient (id, mrn, sex, mother_id, father_id) VALUES
    (1, '99f93d58', 'female', NULL, NULL),
    (2, '28ac2156', 'male', NULL, NULL),
    (3, 'dc6194b7', 'male', 1, 2),
    (4, '3126ce41', 'female', 1, 2);
""")
