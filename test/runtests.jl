#!/usr/bin/env julia

using DataKnots
using DataKnots4Postgres
using NarrativeTest

if !haskey(ENV, "PGHOST") && Sys.islinux()
  if endswith(strip(read(`/usr/bin/lsb_release --id`, String)), "Ubuntu")
     ENV["PGHOST"] = "/var/run/postgresql"
  end
end

args = !isempty(ARGS) ? ARGS : [relpath(joinpath(dirname(abspath(PROGRAM_FILE)), "../doc/src"))]
exit(!runtests(args))
