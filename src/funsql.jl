#
# DSL for assembling SQL queries.
#

struct Table
    scm::Symbol
    name::Symbol
    cols::Vector{Symbol}
end

Table(tbl::PGTable) =
    Table(Symbol(tbl.schema.name), Symbol(tbl.name), Symbol[Symbol(col.name) for col in tbl])

abstract type SQLClause end

Base.getproperty(c::SQLClause, attr::Symbol) =
    pick(c, attr)

Base.getproperty(c::SQLClause, attr::String) =
    pick(c, Symbol(attr))

abstract type SQLExprKind end

struct SQLExpr
    kind::SQLExprKind
    args::Vector{SQLExpr}
end

const EMPTY_SQLEXPR_VECTOR = SQLExpr[]

SQLExpr(kind) =
    SQLExpr(kind, EMPTY_SQLEXPR_VECTOR)

mutable struct Unit <: SQLClause
end

mutable struct From <: SQLClause
    tbl::Table
end

mutable struct Select <: SQLClause
    base::SQLClause
    list::Vector{Pair{Symbol,SQLExpr}}

    Select(base, pairs...) =
        new(base, Pair{Symbol,SQLExpr}[default_alias(p) for p in pairs])
end

mutable struct Where <: SQLClause
    base::SQLClause
    pred::SQLExpr
end

mutable struct Join <: SQLClause
    left::SQLClause
    left_name::Union{Symbol,Nothing}
    right::SQLClause
    right_name::Union{Symbol,Nothing}
    on::SQLExpr

    function Join(left, right, on)
        if left isa SQLClause
            left_name = nothing
        else
            left_name = first(left)
            left = last(left)
        end
        if right isa SQLClause
            right_name = nothing
        else
            right_name = first(right)
            right = last(right)
        end
        new(left, left_name, right, right_name, on)
    end
end

mutable struct Group <: SQLClause
    base::SQLClause
    list::Vector{Pair{Symbol,SQLExpr}}

    Group(base, pairs...) =
        new(base, Pair{Symbol,SQLExpr}[default_alias(p) for p in pairs])
end

struct ConstKind{T} <: SQLExprKind
    val::T
end

struct PickKind <: SQLExprKind
    field::Symbol
    over::SQLClause
end

struct OpKind{S} <: SQLExprKind
end

struct PlaceholderKind <: SQLExprKind
    pos::Int
end

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

struct AggregateKind{S} <: SQLExprKind
    over::SQLClause
end

operation_name(kind::OpKind{S}) where {S} =
    S

operation_name(kind::AggregateKind{S}) where {S} =
    S

Const(val::T) where {T} =
    SQLExpr(ConstKind{T}(val))

Pick(over, field) =
    SQLExpr(PickKind(field, over))

Op(op, args...) =
    SQLExpr(OpKind{op}(), SQLExpr[args...])

Count(; over::SQLClause) =
    SQLExpr(AggregateKind{:COUNT}(over))

Max(val; over::SQLClause) =
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

function pick(@nospecialize(c::SQLClause), s::Symbol)
    val = pick(c, s, nothing)
    val !== nothing || error("cannot found $c")
    val
end

function pick(c::From, s::Symbol, default)
    tbl = getfield(c, :tbl)
    s in tbl.cols ? Pick(c, s) : default
end

function pick(c::Select, s::Symbol, default)
    list = getfield(c, :list)
    findfirst(p -> first(p) === s, list) !== nothing ? Pick(c, s) : default
end

function pick(c::Where, s::Symbol, default)
    base = getfield(c, :base)
    pick(base, s, default)
end

function pick(c::Join, s::Symbol, default)
    left = getfield(c, :left)
    left_name = getfield(c, :left_name)
    right = getfield(c, :right)
    right_name = getfield(c, :right_name)
    if s === left_name
        return left
    elseif s === right_name
        return right
    end
    val = nothing
    if left_name === nothing
        val = pick(left, s, nothing)
    end
    if val === nothing && right_name === nothing
        val = pick(right, s, nothing)
    end
    val !== nothing ? val : default
end

function pick(c::Group, s::Symbol, default)
    base = getfield(c, :base)
    list = getfield(c, :list)
    findfirst(p -> first(p) === s, list) !== nothing ? Pick(c, s) : pick(base, s, default)
end

default_list(@nospecialize ::SQLClause) = SQLExpr[]

default_list(c::From) =
    SQLExpr[Pick(c, col) for col in getfield(c, :tbl).cols]

default_list(c::Select) =
    SQLExpr[Pick(c, alias) for (alias, v) in getfield(c, :list)]

default_list(c::Where) =
    default_list(getfield(c, :base))

default_list(c::Join) =
    vcat(default_list(getfield(c, :left)), default_list(getfield(c, :right)))

default_list(c::Group) =
    SQLExpr[Pick(c, alias) for (alias, v) in getfield(c, :list)]

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

function normalize(@nospecialize c::SQLClause)
    c′, repl = normalize(c, default_list(c))
    c′
end

function normalize(c::Select, refs)
    base = getfield(c, :base)
    list = getfield(c, :list)
    base_refs = collect_refs(list)
    base′, base_repl = normalize(base, base_refs)
    list′ = replace_refs(list, base_repl)
    c′ = Select(base′, list′...)
    repl = Dict{SQLExpr,SQLExpr}()
    for ref in refs
        kind = ref.kind
        if kind isa PickKind && kind.over === c
            repl[ref] = Pick(c′, kind.field)
        end
    end
    c′, repl
end

function normalize(c::Where, refs)
    base = getfield(c, :base)
    pred = getfield(c, :pred)
    base_refs = collect_refs(pred)
    for ref in refs
        push!(base_refs, ref)
    end
    base′, base_repl = normalize(base, base_refs)
    pred′ = replace_refs(pred, base_repl)
    c′ = Where(base′, pred′)
    s = Select(c′)
    list = getfield(s, :list)
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

function normalize(c::Join, refs)
    left = getfield(c, :left)
    right = getfield(c, :right)
    on = getfield(c, :on)
    all_refs = collect_refs(on)
    for ref in refs
        push!(all_refs, ref)
    end
    left′, left_repl = normalize(left, all_refs)
    right′, right_repl = normalize(right, all_refs)
    all_repl = merge(left_repl, right_repl)
    on′ = replace_refs(on, all_repl)
    c′ = Join(left′, right′, on′)
    s = Select(c′)
    list = getfield(s, :list)
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

function normalize(c::From, refs)
    s = Select(c)
    list = getfield(s, :list)
    repl = Dict{SQLExpr,SQLExpr}()
    for ref in refs
        kind = ref.kind
        if kind isa PickKind && kind.over === c
            field = kind.field
            repl[ref] = Pick(s, field)
            push!(list, field => Const(field))
        end
    end
    s, repl
end

function normalize(c::Group, refs)
    base = getfield(c, :base)
    list = getfield(c, :list)
    base_refs = collect_refs(list)
    for ref in refs
        kind = ref.kind
        if kind isa AggregateKind && kind.over === c
            collect_refs!(ref.args, base_refs)
        end
    end
    base′, base_repl = normalize(base, base_refs)
    list′ = replace_refs(list, base_repl)
    c′ = Group(base′, [Const(k) for k = 1:length(list)]...)
    s = Select(c′, list′...)
    list′ = getfield(s, :list)
    repl = Dict{SQLExpr,SQLExpr}()
    pos = 0
    for ref in refs
        kind = ref.kind
        if kind isa PickKind && kind.over === c
            pos += 1
            repl[ref] = Pick(s, kind.field)
        elseif kind isa AggregateKind && kind.over === c
            pos += 1
            field = Symbol("_", pos)
            repl[ref] = Pick(s, field)
            kind′ = AggregateKind{operation_name(kind)}(c′)
            push!(list′, field => SQLExpr(kind′, replace_refs(ref.args, base_repl)))
        end
    end
    s, repl
end

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

function to_sql!(ctx, c::From)
    tbl = getfield(c, :tbl)
    print(ctx, " FROM ")
    to_sql!(ctx, (tbl.scm, tbl.name))
end

function to_sql!(ctx, ::Unit)
end

function to_sql!(ctx, c::Where)
    base = getfield(c, :base)
    pred = getfield(c, :pred)
    print(ctx, " FROM (")
    to_sql!(ctx, base)
    print(ctx, ") AS ")
    to_sql!(ctx, ctx.aliases[base])
    print(ctx, " WHERE ")
    to_sql!(ctx, pred)
end

function to_sql!(ctx, c::Join)
    left = getfield(c, :left)
    right = getfield(c, :right)
    on = getfield(c, :on)
    print(ctx, " FROM (")
    to_sql!(ctx, left)
    print(ctx, ") AS ")
    to_sql!(ctx, ctx.aliases[left])
    print(ctx, " JOIN (")
    to_sql!(ctx, right)
    print(ctx, ") AS ")
    to_sql!(ctx, ctx.aliases[right])
    print(ctx, " ON (")
    to_sql!(ctx, on)
    print(ctx, ")")
end

function to_sql!(ctx, c::Select)
    base = getfield(c, :base)
    list = getfield(c, :list)
    if isempty(ctx.aliases)
        populate_aliases!(ctx, c)
    end
    print(ctx, "SELECT")
    first = true
    for (alias, val) in list
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
    if base isa Select
        print(ctx, " FROM (")
        to_sql!(ctx, base)
        print(ctx, ") AS ")
        to_sql!(ctx, ctx.aliases[base])
    else
        to_sql!(ctx, base)
    end
end

function to_sql!(ctx, c::Group)
    base = getfield(c, :base)
    list = getfield(c, :list)
    print(ctx, " FROM (")
    to_sql!(ctx, base)
    print(ctx, ") AS ")
    to_sql!(ctx, ctx.aliases[base])
    print(ctx, " GROUP BY")
    first = true
    for (alias, val) in list
        if first
            print(ctx, ' ')
            first = false
        else
            print(ctx, ", ")
        end
        to_sql!(ctx, val)
    end
end

function populate_aliases!(ctx, c::Select)
    base = getfield(c, :base)
    ctx.aliases[c] = gensym()
    populate_aliases!(ctx, base)
end

function populate_aliases!(ctx, ::Union{From,Unit})
end

function populate_aliases!(ctx, c::Where)
    base = getfield(c, :base)
    populate_aliases!(ctx, base)
end

function populate_aliases!(ctx, c::Join)
    left = getfield(c, :left)
    right = getfield(c, :right)
    populate_aliases!(ctx, left)
    populate_aliases!(ctx, right)
end

function populate_aliases!(ctx, c::Group)
    base = getfield(c, :base)
    populate_aliases!(ctx, base)
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

