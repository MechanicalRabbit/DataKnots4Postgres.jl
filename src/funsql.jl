#
# DSL for assembling SQL queries.
#

# SQL Catalog

struct SQLTable
    scm::Symbol
    name::Symbol
    cols::Vector{Symbol}
end

SQLTable(tbl::PGTable) =
    SQLTable(Symbol(tbl.schema.name), Symbol(tbl.name), Symbol[Symbol(col.name) for col in tbl])

# Queries

abstract type SQLQueryKind end

struct SQLQuery
    kind::SQLQueryKind
    args::Vector{SQLQuery}
end

struct TransparentSQLQuery{K}
    kind::K
    args::Vector{SQLQuery}
end

transparent(q::SQLQuery) =
    TransparentSQLQuery(getfield(q, :kind), getfield(q, :args))

opaque(q::TransparentSQLQuery) =
    SQLQuery(q.kind, q.args)

const EMPTY_SQLQUERY_VECTOR = SQLQuery[]

SQLQuery(kind::SQLQueryKind) =
    SQLQuery(kind, EMPTY_SQLQUERY_VECTOR)

SQLQuery(kind::SQLQueryKind, arg::SQLQuery) =
    SQLQuery(kind, [arg])

struct SQLQueryClosure
    kind::SQLQueryKind
    args::Vector{SQLQuery}
end

SQLQueryClosure(kind::SQLQueryKind) =
    SQLQueryClosure(kind, EMPTY_SQLQUERY_VECTOR)

SQLQueryClosure(kind::SQLQueryKind, arg::SQLQuery) =
    SQLQueryClosure(kind, [arg])

(c::SQLQueryClosure)(q::SQLQuery) =
    SQLQuery(c.kind, [q, c.args...])

# Expressions

abstract type SQLExprKind end

struct SQLExpr
    kind::SQLExprKind
    args::Vector{SQLExpr}
end

const EMPTY_SQLEXPR_VECTOR = SQLExpr[]

SQLExpr(kind) =
    SQLExpr(kind, EMPTY_SQLEXPR_VECTOR)

# Query kinds

# Unit

mutable struct UnitKind <: SQLQueryKind
end

Unit() = SQLQuery(UnitKind())

# From

mutable struct FromKind <: SQLQueryKind
    tbl::SQLTable
end

From(tbl::SQLTable) =
    SQLQuery(FromKind(tbl))

# Select

mutable struct SelectKind <: SQLQueryKind
    list::Vector{Pair{Symbol,SQLExpr}}
end

Select(q::SQLQuery, list::Vector{Pair{Symbol,SQLExpr}}) =
    SQLQuery(SelectKind(list), q)

Select(list::Vector{Pair{Symbol,SQLExpr}}) =
    SQLQueryClosure(SelectKind(list))

Select(q::SQLQuery, list...) =
    Select(q, default_alias(list))

Select(list...) =
    Select(default_alias(list))

# Where

mutable struct WhereKind <: SQLQueryKind
    pred::SQLExpr
end

Where(q::SQLQuery, pred::SQLExpr) =
    SQLQuery(WhereKind(pred), q)

Where(pred::SQLExpr) =
    SQLQueryClosure(WhereKind(pred))

# Join

mutable struct JoinKind <: SQLQueryKind
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
    SQLQuery(JoinKind(left_name, right_name, on), [left, right])
end

function Join(right::Union{Pair{Symbol,SQLQuery},SQLQuery},
              on::SQLExpr)
    if right isa SQLQuery
        right_name = nothing
    else
        right_name = first(right)
        right = last(right)
    end
    SQLQueryClosure(JoinKind(nothing, right_name, on), [right])
end

# Group

mutable struct GroupKind <: SQLQueryKind
    list::Vector{Pair{Symbol,SQLExpr}}
end

Group(q::SQLQuery, list::Vector{Pair{Symbol,SQLExpr}}) =
    SQLQuery(GroupKind(list), q)

Group(list::Vector{Pair{Symbol,SQLExpr}}) =
    SQLQueryClosure(GroupKind(list))

Group(q::SQLQuery, list...) =
    Group(q, default_alias(list))

Group(list...) =
    Group(default_alias(list))

# Expressions

struct ConstKind{T} <: SQLExprKind
    val::T
end

struct PickKind <: SQLExprKind
    field::Symbol
    over::SQLQuery
end

struct OpKind{S} <: SQLExprKind
end

struct AggregateKind{S} <: SQLExprKind
    over::SQLQuery
end

struct PlaceholderKind <: SQLExprKind
    pos::Int
end


#=
abstract type SQLExpr end

struct Const{T} <: SQLExpr
    val::T
end

struct Pick <: SQLExpr
    field::Symbol
    over::SQLQuery
end

struct Op{S} <: SQLExpr
    args::Vector{SQLExpr}
end

struct Aggregate{S} <: SQLExpr
    over::SQLQuery
    args::Vector{SQLExpr}
end

struct Placeholder <: SQLExpr
    pos::Int
end

Base.iterate(::SQLExpr) =
    nothing

Base.iterate(ex::Union{Op,Aggregate}, state=1) =
    iterate(ex.args, state)
=#


#=
struct OpLookup
end

Base.getproperty(::OpLookup, S::Symbol) =
    (args...) -> SQLExpr(OpKind(S){}, SQLExpr[args...])

const Op = OpLookup()

Op.CONCAT("Now is ", Op.NOW())

Agg.COUNT()

Agg.MAX()

to_sql(ctx, ::PostresDialect, ::OpKind{:CONCAT}, args) =
    print(ctx, "args[1] || args[2]")
=#

Base.getindex(q::SQLQuery, attr::Symbol) =
    pick(q, attr)

Base.getindex(q::SQLQuery, attr::String) =
    pick(q, Symbol(attr))

Base.getproperty(q::SQLQuery, attr::Symbol) =
    pick(q, attr)

Base.getproperty(q::SQLQuery, attr::String) =
    pick(q, Symbol(attr))

operation_name(kind::OpKind{S}) where {S} =
    S

operation_name(kind::AggregateKind{S}) where {S} =
    S

Const(val::T) where {T} =
    SQLExpr(ConstKind{T}(val))

Pick(over::SQLQuery, field) =
    SQLExpr(PickKind(field, over))

Pick(kind::SQLQueryKind, args::Vector{SQLQuery}, field) =
    Pick(SQLQuery(kind, args), field)

Op(op, args...) =
    SQLExpr(OpKind{op}(), SQLExpr[args...])

Count(; over::SQLQuery) =
    SQLExpr(AggregateKind{:COUNT}(over))

Max(val; over::SQLQuery) =
    SQLExpr(AggregateKind{:MAX}(over), SQLExpr[val])

Placeholder(pos) =
    SQLExpr(PlaceholderKind(pos))

function collect_refs(ex)
    refs = Set{SQLExpr}()
    collect_refs!(ex, refs)
    refs
end

function collect_refs!(ex::SQLExpr, refs)
    if ex.kind isa PickKind || ex.kind isa AggregateKind
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
    if ex.kind isa PickKind || ex.kind isa AggregateKind
        return get(repl, ex, ex)
    end
    args′ = replace_refs(ex.args, repl)
    SQLExpr(ex.kind, args′)
end

replace_refs(exs::Vector{SQLExpr}, repl) =
    SQLExpr[replace_refs(ex, repl) for ex in exs]

function pick(q::SQLQuery, s::Symbol)
    val = pick(q, s, nothing)
    val !== nothing || error("cannot found $s")
    val
end

function pick(q::SQLQuery, s::Symbol, default)
    kind = getfield(q, :kind)
    args = getfield(q, :args)
    pick(kind, args, s, default)
end

pick(kind::SQLQueryKind, args, s, default) =
    default

function pick(kind::FromKind, args, s, default)
    s in kind.tbl.cols ? Pick(kind, args, s) : default
end

function pick(kind::SelectKind, args, s, default)
    findfirst(p -> first(p) === s, kind.list) !== nothing ? Pick(kind, args, s) : default
end

function pick(kind::WhereKind, args, s, default)
    base, = args
    pick(base, s, default)
end

function pick(kind::JoinKind, args, s, default)
    left, right = args
    if s === kind.left_name
        return left
    elseif s === kind.right_name
        return right
    end
    val = nothing
    if kind.left_name === nothing
        val = pick(left, s, nothing)
    end
    if val === nothing && kind.right_name === nothing
        val = pick(right, s, nothing)
    end
    val !== nothing ? val : default
end

function pick(kind::GroupKind, args, s, default)
    base, = args
    findfirst(p -> first(p) === s, kind.list) !== nothing ? Pick(kind, args, s) : pick(base, s, default)
end

default_list(q::SQLQuery) =
    default_list(getfield(q, :kind), getfield(q, :args))

default_list(::SQLQueryKind, args) =
    SQLExpr[]

default_list(kind::FromKind, args) =
    SQLExpr[Pick(kind, args, col) for col in kind.tbl.cols]

default_list(kind::SelectKind, args) =
    SQLExpr[Pick(kind, args, alias) for (alias, v) in kind.list]

function default_list(kind::WhereKind, args)
    base, = args
    default_list(base)
end

function default_list(kind::JoinKind, args)
    #=
    left, right = args
    SQLExpr[default_list(left)..., default_list(right)...]
    =#
    left, right = args
    list = SQLExpr[]
    if kind.left_name === nothing
        append!(list, default_list(left))
    end
    if kind.right_name === nothing
        append!(list, default_list(right))
    end
    list
end

default_list(kind::GroupKind, args) =
    SQLExpr[Pick(kind, args, alias) for (alias, v) in kind.list]

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
    default_alias(ex.kind) => ex

default_alias(kind::PickKind) =
    kind.field

default_alias(kind::ConstKind) =
    Symbol(kind.val)

default_alias(kind::OpKind{S}) where {S} =
    S

default_alias(kind::AggregateKind{S}) where {S} =
    S

function normalize(q::SQLQuery)
    q′, repl = normalize(q, default_list(q))
    q′
end

normalize(q::SQLQuery, refs) =
    normalize(getfield(q, :kind), getfield(q, :args), refs)

function normalize(kind::SelectKind, args, refs)
    base, = args
    base_refs = collect_refs(kind.list)
    base′, base_repl = normalize(base, base_refs)
    list′ = replace_refs(kind.list, base_repl)
    c′ = Select(base′, list′)
    repl = Dict{SQLExpr,SQLExpr}()
    for ref in refs
        ref_kind = ref.kind
        if ref_kind isa PickKind && getfield(ref_kind.over, :kind) === kind
            repl[ref] = Pick(c′, ref_kind.field)
        end
    end
    c′, repl
end

function normalize(kind::WhereKind, args, refs)
    base, = args
    base_refs = collect_refs(kind.pred)
    for ref in refs
        push!(base_refs, ref)
    end
    base′, base_repl = normalize(base, base_refs)
    pred′ = replace_refs(kind.pred, base_repl)
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

function normalize(kind::JoinKind, args, refs)
    left, right = args
    all_refs = collect_refs(kind.on)
    for ref in refs
        push!(all_refs, ref)
    end
    left′, left_repl = normalize(left, all_refs)
    right′, right_repl = normalize(right, all_refs)
    all_repl = merge(left_repl, right_repl)
    on′ = replace_refs(kind.on, all_repl)
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

function normalize(kind::FromKind, args, refs)
    list = Pair{Symbol,SQLExpr}[]
    s = Select(SQLQuery(kind, args), list)
    repl = Dict{SQLExpr,SQLExpr}()
    for ref in refs
        ref_kind = ref.kind
        if ref_kind isa PickKind && getfield(ref_kind.over, :kind) === kind
            field = ref_kind.field
            repl[ref] = Pick(s, field)
            push!(list, field => Const(field))
        end
    end
    s, repl
end

function normalize(kind::GroupKind, args, refs)
    base, = args
    base_refs = collect_refs(kind.list)
    for ref in refs
        ref_kind = ref.kind
        if ref_kind isa AggregateKind && getfield(ref_kind.over, :kind) === kind
            collect_refs!(ref.args, base_refs)
        end
    end
    base′, base_repl = normalize(base, base_refs)
    list′ = replace_refs(kind.list, base_repl)
    c′ = Group(base′, Pair{Symbol,SQLExpr}[Symbol("_", k) => Const(k) for k = 1:length(kind.list)])
    s = Select(c′, list′)
    repl = Dict{SQLExpr,SQLExpr}()
    pos = 0
    for ref in refs
        ref_kind = ref.kind
        if ref_kind isa PickKind && getfield(ref_kind.over, :kind) === kind
            pos += 1
            repl[ref] = Pick(s, ref_kind.field)
        elseif ref_kind isa AggregateKind && getfield(ref_kind.over, :kind) === kind
            pos += 1
            field = Symbol("_", pos)
            repl[ref] = Pick(s, field)
            kind′ = AggregateKind{operation_name(ref_kind)}(c′)
            push!(list′, field => SQLExpr(kind′, replace_refs(ref.args, base_repl)))
        end
    end
    s, repl
end

to_sql!(ctx, q::SQLQuery) =
    to_sql!(ctx, getfield(q, :kind), getfield(q, :args))

to_sql!(ctx, ex::SQLExpr) =
    to_sql!(ctx, ex.kind, ex.args)

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

to_sql!(ctx, kind::ConstKind, args) =
    to_sql!(ctx, kind.val)

to_sql!(ctx, kind::PickKind, args) =
    to_sql!(ctx, (ctx.aliases[kind.over], kind.field))

function to_sql!(ctx, @nospecialize(kind::AggregateKind{S}), args) where {S}
    print(ctx, S)
    to_sql!(ctx, args)
end

to_sql!(ctx, kind::AggregateKind{:COUNT}, args) =
    print(ctx, "COUNT(TRUE)")

function to_sql!(ctx, @nospecialize(kind::OpKind{S}), args) where {S}
    print(ctx, S)
    to_sql!(ctx, args)
end

function to_sql!(ctx, kind::OpKind{:(=)}, args)
    to_sql!(ctx, args, " = ")
end

function to_sql!(ctx, kind::OpKind{:(&&)}, args)
    if isempty(args)
        print(ctx, "TRUE")
    else
        to_sql!(ctx, args, " AND ")
    end
end

function to_sql!(ctx, kind::PlaceholderKind, args)
    print(ctx, '$')
    to_sql!(ctx, kind.pos)
end

function to_sql!(ctx, kind::FromKind, args)
    tbl = kind.tbl
    print(ctx, " FROM ")
    to_sql!(ctx, (tbl.scm, tbl.name))
end

function to_sql!(ctx, ::UnitKind, args)
end

function to_sql!(ctx, kind::WhereKind, args)
    base, = args
    print(ctx, " FROM (")
    to_sql!(ctx, base)
    print(ctx, ") AS ")
    to_sql!(ctx, ctx.aliases[base])
    print(ctx, " WHERE ")
    to_sql!(ctx, kind.pred)
end

function to_sql!(ctx, kind::JoinKind, args)
    left, right = args
    print(ctx, " FROM (")
    to_sql!(ctx, left)
    print(ctx, ") AS ")
    to_sql!(ctx, ctx.aliases[left])
    print(ctx, " JOIN (")
    to_sql!(ctx, right)
    print(ctx, ") AS ")
    to_sql!(ctx, ctx.aliases[right])
    print(ctx, " ON (")
    to_sql!(ctx, kind.on)
    print(ctx, ")")
end

function to_sql!(ctx, kind::SelectKind, args)
    base, = args
    if isempty(ctx.aliases)
        populate_aliases!(ctx, args)
    end
    print(ctx, "SELECT")
    first = true
    for (alias, val) in kind.list
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
    if getfield(base, :kind) isa SelectKind
        print(ctx, " FROM (")
        to_sql!(ctx, base)
        print(ctx, ") AS ")
        to_sql!(ctx, ctx.aliases[base])
    else
        to_sql!(ctx, base)
    end
end

function to_sql!(ctx, kind::GroupKind, args)
    base, = args
    print(ctx, " FROM (")
    to_sql!(ctx, base)
    print(ctx, ") AS ")
    to_sql!(ctx, ctx.aliases[base])
    print(ctx, " GROUP BY")
    first = true
    for (alias, val) in kind.list
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
    base_kind = getfield(q, :kind)
    if base_kind isa SelectKind
        ctx.aliases[q] = gensym()
    end
    populate_aliases!(ctx, getfield(q, :args))
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

