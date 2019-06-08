#!/usr/bin/env julia

using Pkg
haskey(Pkg.installed(), "Documenter") || Pkg.add("Documenter")

using Documenter
using DataKnots4Postgres

# Highlight indented code blocks as Julia code.
using Markdown
Markdown.Code(code) = Markdown.Code("julia", code)

makedocs(
    sitename = "DataKnots4Postgres.jl",
    pages = [
        "Home" => "index.md",
    ],
    modules = [DataKnots4Postgres])

deploydocs(
    repo = "github.com/rbt-lang/DataKnots4Postgres.jl.git",
)
