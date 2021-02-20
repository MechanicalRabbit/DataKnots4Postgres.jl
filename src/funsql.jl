#
# DSL for assembling SQL queries.
#

# SQL Catalog

mutable struct SQLTable
    scm::Symbol
    name::Symbol
    cols::Vector{Symbol}
end

SQLTable(tbl::PGTable) =
    SQLTable(Symbol(tbl.schema.name), Symbol(tbl.name), Symbol[Symbol(col.name) for col in tbl])

# Queries

abstract type SQLQueryCore end

mutable struct SQLQuery
    core::SQLQueryCore
    args::Vector{SQLQuery}
end

const EMPTY_SQLQUERY_VECTOR = SQLQuery[]

SQLQuery(core::SQLQueryCore) =
    SQLQuery(core, EMPTY_SQLQUERY_VECTOR)

SQLQuery(core::SQLQueryCore, arg::SQLQuery) =
    SQLQuery(core, [arg])

struct SQLQueryClosure
    core::SQLQueryCore
    args::Vector{SQLQuery}
end

SQLQueryClosure(core::SQLQueryCore) =
    SQLQueryClosure(core, EMPTY_SQLQUERY_VECTOR)

SQLQueryClosure(core::SQLQueryCore, arg::SQLQuery) =
    SQLQueryClosure(core, [arg])

(c::SQLQueryClosure)(q::SQLQuery) =
    SQLQuery(c.core, [q, c.args...])

Base.length(q::SQLQuery) =
    length(q.args)

Base.iterate(q::SQLQuery, state=1) =
    iterate(q.args, state)

# Expressions

abstract type SQLExprCore end

struct SQLExpr
    core::SQLExprCore
    args::Vector{SQLExpr}
end

const EMPTY_SQLEXPR_VECTOR = SQLExpr[]

SQLExpr(core) =
    SQLExpr(core, EMPTY_SQLEXPR_VECTOR)

Base.length(q::SQLExpr) =
    length(q.args)

Base.iterate(q::SQLExpr, state=1) =
    iterate(q.args, state)

# Query cores

# Unit

struct UnitCore <: SQLQueryCore
end

Unit() = SQLQuery(UnitCore())

# From

struct FromCore <: SQLQueryCore
    tbl::SQLTable
end

From(tbl::SQLTable) =
    SQLQuery(FromCore(tbl))

# Select

struct SelectCore <: SQLQueryCore
    list::Vector{Pair{Symbol,SQLExpr}}
end

Select(q::SQLQuery, list::Vector{Pair{Symbol,SQLExpr}}) =
    SQLQuery(SelectCore(list), q)

Select(list::Vector{Pair{Symbol,SQLExpr}}) =
    SQLQueryClosure(SelectCore(list))

Select(q::SQLQuery, list...) =
    Select(q, default_alias(list))

Select(list...) =
    Select(default_alias(list))

# Where

struct WhereCore <: SQLQueryCore
    pred::SQLExpr
end

Where(q::SQLQuery, pred::SQLExpr) =
    SQLQuery(WhereCore(pred), q)

Where(pred::SQLExpr) =
    SQLQueryClosure(WhereCore(pred))

# Join

struct JoinCore <: SQLQueryCore
    left_name::Union{Symbol,Nothing}
    right_name::Union{Symbol,Nothing}
    on::SQLExpr
end

function Join(left::Union{Pair{Symbol,SQLQuery},SQLQuery},
              right::Union{Pair{Symbol,SQLQuery},SQLQuery},
              on::SQLExpr)
    if left isa SQLQuery
        left_name = nothing
    else
        left_name = first(left)
        left = last(left)
    end
    if right isa SQLQuery
        right_name = nothing
    else
        right_name = first(right)
        right = last(right)
    end
    SQLQuery(JoinCore(left_name, right_name, on), [left, right])
end

function Join(right::Union{Pair{Symbol,SQLQuery},SQLQuery},
              on::SQLExpr)
    if right isa SQLQuery
        right_name = nothing
    else
        right_name = first(right)
        right = last(right)
    end
    SQLQueryClosure(JoinCore(nothing, right_name, on), [right])
end

# Group

struct GroupCore <: SQLQueryCore
    list::Vector{Pair{Symbol,SQLExpr}}
end

Group(q::SQLQuery, list::Vector{Pair{Symbol,SQLExpr}}) =
    SQLQuery(GroupCore(list), q)

Group(list::Vector{Pair{Symbol,SQLExpr}}) =
    SQLQueryClosure(GroupCore(list))

Group(q::SQLQuery, list...) =
    Group(q, default_alias(list))

Group(list...) =
    Group(default_alias(list))

# Expressions

struct ConstCore{T} <: SQLExprCore
    val::T
end

struct PickCore <: SQLExprCore
    field::Symbol
    over::SQLQuery
end

struct OpCore{S} <: SQLExprCore
end

struct AggregateCore{S} <: SQLExprCore
    over::SQLQuery
end

struct PlaceholderCore <: SQLExprCore
    pos::Int
end


#=
struct OpLookup
end

Base.getproperty(::OpLookup, S::Symbol) =
    (args...) -> SQLExpr(OpCore(S){}, SQLExpr[args...])

const Op = OpLookup()

Op.CONCAT("Now is ", Op.NOW())

Agg.COUNT()

Agg.MAX()

to_sql(ctx, ::PostresDialect, ::OpCore{:CONCAT}, args) =
    print(ctx, "args[1] || args[2]")
=#

Base.getindex(q::SQLQuery, attr::Symbol) =
    pick(q, attr)

Base.getindex(q::SQLQuery, attr::String) =
    pick(q, Symbol(attr))

operation_name(core::OpCore{S}) where {S} =
    S

operation_name(core::AggregateCore{S}) where {S} =
    S

Const(val::T) where {T} =
    SQLExpr(ConstCore{T}(val))

Pick(over::SQLQuery, field) =
    SQLExpr(PickCore(field, over))

Op(op, args...) =
    SQLExpr(OpCore{op}(), SQLExpr[args...])

Count(; over::SQLQuery) =
    SQLExpr(AggregateCore{:COUNT}(over))

Max(val; over::SQLQuery) =
    SQLExpr(AggregateCore{:MAX}(over), SQLExpr[val])

Placeholder(pos) =
    SQLExpr(PlaceholderCore(pos))

function collect_refs(ex)
    refs = Set{SQLExpr}()
    collect_refs!(ex, refs)
    refs
end

function collect_refs!(ex::SQLExpr, refs)
    if ex.core isa PickCore || ex.core isa AggregateCore
        push!(refs, ex)
    end
    collect_refs!(ex.args, refs)
end

function collect_refs!(exs::Vector{SQLExpr}, refs)
    for ex in exs
        collect_refs!(ex, refs)
    end
end

function collect_refs!(list::Vector{Pair{Symbol,SQLExpr}}, refs)
    for (alias, ex) in list
        collect_refs!(ex, refs)
    end
end

replace_refs(list::Vector{Pair{Symbol,SQLExpr}}, repl) =
    Pair{Symbol,SQLExpr}[alias => replace_refs(ex, repl) for (alias, ex) in list]

function replace_refs(ex::SQLExpr, repl)
    if ex.core isa PickCore || ex.core isa AggregateCore
        return get(repl, ex, ex)
    end
    args′ = replace_refs(ex.args, repl)
    SQLExpr(ex.core, args′)
end

replace_refs(exs::Vector{SQLExpr}, repl) =
    SQLExpr[replace_refs(ex, repl) for ex in exs]

function pick(q::SQLQuery, s::Symbol)
    val = pick(q, s, nothing)
    val !== nothing || error("cannot found $s")
    val
end

function pick(q::SQLQuery, s::Symbol, default)
    pick(q.core, q, s, default)
end

pick(core::SQLQueryCore, q, s, default) =
    default

function pick(core::FromCore, q, s, default)
    s in core.tbl.cols ? Pick(q, s) : default
end

function pick(core::SelectCore, q, s, default)
    findfirst(p -> first(p) === s, core.list) !== nothing ? Pick(q, s) : default
end

function pick(core::WhereCore, q, s, default)
    base, = q
    pick(base, s, default)
end

function pick(core::JoinCore, q, s, default)
    left, right = q
    if s === core.left_name
        return left
    elseif s === core.right_name
        return right
    end
    val = nothing
    if core.left_name === nothing
        val = pick(left, s, nothing)
    end
    if val === nothing && core.right_name === nothing
        val = pick(right, s, nothing)
    end
    val !== nothing ? val : default
end

function pick(core::GroupCore, q, s, default)
    base, = q
    findfirst(p -> first(p) === s, core.list) !== nothing ? Pick(q, s) : pick(base, s, default)
end

default_list(q::SQLQuery) =
    default_list(q.core, q)

default_list(::SQLQueryCore, q) =
    SQLExpr[]

default_list(core::FromCore, q) =
    SQLExpr[Pick(q, col) for col in core.tbl.cols]

default_list(core::SelectCore, q) =
    SQLExpr[Pick(q, alias) for (alias, v) in core.list]

function default_list(core::WhereCore, q)
    base, = q
    default_list(base)
end

function default_list(core::JoinCore, q)
    #=
    left, right = q
    SQLExpr[default_list(left)..., default_list(right)...]
    =#
    left, right = q
    list = SQLExpr[]
    if core.left_name === nothing
        append!(list, default_list(left))
    end
    if core.right_name === nothing
        append!(list, default_list(right))
    end
    list
end

default_list(core::GroupCore, q) =
    SQLExpr[Pick(q, alias) for (alias, v) in core.list]

function default_alias(@nospecialize list)
    list′ = Pair{Symbol,SQLExpr}[]
    seen = Set{Symbol}()
    for p in list
        p′= default_alias(p)
        !(first(p′) in seen) || error("duplicate alias $(first(p′))")
        push!(list′, p′)
        push!(seen, first(p′))
    end
    list′
end

default_alias(p::Pair) = p

default_alias(ex::SQLExpr) =
    default_alias(ex.core) => ex

default_alias(core::PickCore) =
    core.field

default_alias(core::ConstCore) =
    Symbol(core.val)

default_alias(core::OpCore{S}) where {S} =
    S

default_alias(core::AggregateCore{S}) where {S} =
    S

function normalize(q::SQLQuery)
    q′, repl = normalize(q, default_list(q))
    q′
end

normalize(q::SQLQuery, refs) =
    normalize(q.core, q, refs)

function normalize(core::SelectCore, q, refs)
    base, = q
    base_refs = collect_refs(core.list)
    base′, base_repl = normalize(base, base_refs)
    list′ = replace_refs(core.list, base_repl)
    c′ = Select(base′, list′)
    repl = Dict{SQLExpr,SQLExpr}()
    for ref in refs
        ref_core = ref.core
        if ref_core isa PickCore && ref_core.over.core === core
            repl[ref] = Pick(c′, ref_core.field)
        end
    end
    c′, repl
end

function normalize(core::WhereCore, q, refs)
    base, = q
    base_refs = collect_refs(core.pred)
    for ref in refs
        push!(base_refs, ref)
    end
    base′, base_repl = normalize(base, base_refs)
    pred′ = replace_refs(core.pred, base_repl)
    c′ = Where(base′, pred′)
    list = Pair{Symbol,SQLExpr}[]
    s = Select(c′, list)
    repl = Dict{SQLExpr,SQLExpr}()
    pos = 0
    for ref in refs
        if ref in keys(base_repl)
            pos += 1
            field = Symbol("_", pos)
            repl[ref] = Pick(s, field)
            push!(list, field => base_repl[ref])
        end
    end
    s, repl
end

function normalize(core::JoinCore, q, refs)
    left, right = q
    all_refs = collect_refs(core.on)
    for ref in refs
        push!(all_refs, ref)
    end
    left′, left_repl = normalize(left, all_refs)
    right′, right_repl = normalize(right, all_refs)
    all_repl = merge(left_repl, right_repl)
    on′ = replace_refs(core.on, all_repl)
    c′ = Join(left′, right′, on′)
    list = Pair{Symbol,SQLExpr}[]
    s = Select(c′, list)
    repl = Dict{SQLExpr,SQLExpr}()
    pos = 0
    for ref in refs
        if ref in keys(all_repl)
            pos += 1
            field = Symbol("_", pos)
            repl[ref] = Pick(s, field)
            push!(list, field => all_repl[ref])
        end
    end
    s, repl
end

function normalize(core::FromCore, q, refs)
    list = Pair{Symbol,SQLExpr}[]
    s = Select(q, list)
    repl = Dict{SQLExpr,SQLExpr}()
    for ref in refs
        ref_core = ref.core
        if ref_core isa PickCore && ref_core.over.core === core
            field = ref_core.field
            repl[ref] = Pick(s, field)
            push!(list, field => Const(field))
        end
    end
    s, repl
end

function normalize(core::GroupCore, q, refs)
    base, = q
    base_refs = collect_refs(core.list)
    for ref in refs
        ref_core = ref.core
        if ref_core isa AggregateCore && ref_core.over.core === core
            collect_refs!(ref.args, base_refs)
        end
    end
    base′, base_repl = normalize(base, base_refs)
    list′ = replace_refs(core.list, base_repl)
    c′ = Group(base′, Pair{Symbol,SQLExpr}[Symbol("_", k) => Const(k) for k = 1:length(core.list)])
    s = Select(c′, list′)
    repl = Dict{SQLExpr,SQLExpr}()
    pos = 0
    for ref in refs
        ref_core = ref.core
        if ref_core isa PickCore && ref_core.over.core === core
            pos += 1
            repl[ref] = Pick(s, ref_core.field)
        elseif ref_core isa AggregateCore && ref_core.over.core === core
            pos += 1
            field = Symbol("_", pos)
            repl[ref] = Pick(s, field)
            core′ = AggregateCore{operation_name(ref_core)}(c′)
            push!(list′, field => SQLExpr(core′, replace_refs(ref.args, base_repl)))
        end
    end
    s, repl
end

to_sql!(ctx, q::SQLQuery) =
    to_sql!(ctx, q.core, q)

to_sql!(ctx, ex::SQLExpr) =
    to_sql!(ctx, ex.core, ex)

function to_sql!(ctx, exs::Vector{SQLExpr}, sep=", ", left="(", right=")")
    print(ctx, left)
    first = true
    for ex in exs
        if !first
            print(ctx, sep)
        else
            first = false
        end
        to_sql!(ctx, ex)
    end
    print(ctx, right)
end

to_sql!(ctx, core::ConstCore, ex) =
    to_sql!(ctx, core.val)

to_sql!(ctx, core::PickCore, ex) =
    to_sql!(ctx, (ctx.aliases[core.over], core.field))

function to_sql!(ctx, @nospecialize(core::AggregateCore{S}), ex) where {S}
    print(ctx, S)
    to_sql!(ctx, ex.args)
end

to_sql!(ctx, core::AggregateCore{:COUNT}, ex) =
    print(ctx, "COUNT(TRUE)")

function to_sql!(ctx, @nospecialize(core::OpCore{S}), ex) where {S}
    print(ctx, S)
    to_sql!(ctx, ex.args)
end

function to_sql!(ctx, core::OpCore{:(=)}, ex)
    to_sql!(ctx, ex.args, " = ")
end

function to_sql!(ctx, core::OpCore{:(&&)}, ex)
    if isempty(ex.args)
        print(ctx, "TRUE")
    else
        to_sql!(ctx, ex.args, " AND ")
    end
end

function to_sql!(ctx, core::PlaceholderCore, ex)
    print(ctx, '$')
    to_sql!(ctx, core.pos)
end

function to_sql!(ctx, core::FromCore, q)
    tbl = core.tbl
    print(ctx, " FROM ")
    to_sql!(ctx, (tbl.scm, tbl.name))
end

function to_sql!(ctx, ::UnitCore, q)
end

function to_sql!(ctx, core::WhereCore, q)
    base, = q
    print(ctx, " FROM (")
    to_sql!(ctx, base)
    print(ctx, ") AS ")
    to_sql!(ctx, ctx.aliases[base])
    print(ctx, " WHERE ")
    to_sql!(ctx, core.pred)
end

function to_sql!(ctx, core::JoinCore, q)
    left, right = q
    print(ctx, " FROM (")
    to_sql!(ctx, left)
    print(ctx, ") AS ")
    to_sql!(ctx, ctx.aliases[left])
    print(ctx, " JOIN (")
    to_sql!(ctx, right)
    print(ctx, ") AS ")
    to_sql!(ctx, ctx.aliases[right])
    print(ctx, " ON (")
    to_sql!(ctx, core.on)
    print(ctx, ")")
end

function to_sql!(ctx, core::SelectCore, q)
    base, = q
    if isempty(ctx.aliases)
        populate_aliases!(ctx, q)
    end
    print(ctx, "SELECT")
    first = true
    for (alias, val) in core.list
        if first
            print(ctx, ' ')
            first = false
        else
            print(ctx, ", ")
        end
        to_sql!(ctx, val)
        print(ctx, " AS ")
        to_sql!(ctx, alias)
    end
    if base.core isa SelectCore
        print(ctx, " FROM (")
        to_sql!(ctx, base)
        print(ctx, ") AS ")
        to_sql!(ctx, ctx.aliases[base])
    else
        to_sql!(ctx, base)
    end
end

function to_sql!(ctx, core::GroupCore, q)
    base, = q
    print(ctx, " FROM (")
    to_sql!(ctx, base)
    print(ctx, ") AS ")
    to_sql!(ctx, ctx.aliases[base])
    print(ctx, " GROUP BY")
    first = true
    for (alias, val) in core.list
        if first
            print(ctx, ' ')
            first = false
        else
            print(ctx, ", ")
        end
        to_sql!(ctx, val)
    end
end

function populate_aliases!(ctx, qs::Vector{SQLQuery})
    for q in qs
        populate_aliases!(ctx, q)
    end
end

function populate_aliases!(ctx, q::SQLQuery)
    base_core = q.core
    if base_core isa SelectCore
        ctx.aliases[q] = gensym()
    end
    populate_aliases!(ctx, q.args)
end

mutable struct ToSQLContext <: IO
    io::IOBuffer
    aliases::Dict{Any,Symbol}       # Dict{Select,Symbol}

    ToSQLContext() =
        new(IOBuffer(), Dict{Any,Symbol}())
end

Base.write(ctx::ToSQLContext, octet::UInt8) =
    write(ctx.io, octet)

Base.unsafe_write(ctx::ToSQLContext, input::Ptr{UInt8}, nbytes::UInt) =
    unsafe_write(ctx.io, input, nbytes)

function to_sql(@nospecialize ex)
    ctx = ToSQLContext()
    to_sql!(ctx, ex)
    String(take!(ctx.io))
end

function to_sql!(ctx, ::Missing)
    print(ctx, "NULL")
end

function to_sql!(ctx, b::Bool)
    print(ctx, b ? "TRUE" : "FALSE")
end

function to_sql!(ctx, n::Number)
    print(ctx, n)
end

function to_sql!(ctx, s::AbstractString)
    print(ctx, '\'', replace(s, '\'' => "''"), '\'')
end

function to_sql!(ctx, n::Symbol)
    print(ctx, '"', replace(string(n), '"' => "\"\""), '"')
end

function to_sql!(ctx, qn::Tuple{Symbol,Symbol})
    to_sql!(ctx, qn[1])
    print(ctx, '.')
    to_sql!(ctx, qn[2])
end

