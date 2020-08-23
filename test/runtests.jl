#!/usr/bin/env julia

using DataKnots
using DataKnots4Postgres
using NarrativeTest

args = !isempty(ARGS) ? ARGS : [relpath(joinpath(dirname(abspath(PROGRAM_FILE)), "../doc/src"))]

withenv("LINES" => 11, "COLUMNS" => 72) do
    exit(!runtests(args))
end
