#
# DSL for assembling SQL queries.
#

struct Table
    scm::Symbol
    name::Symbol
    cols::Vector{Symbol}
end

abstract type SQLClause end

Base.getproperty(c::SQLClause, attr::Symbol) =
    Pick(c, attr)

Base.getproperty(c::SQLClause, attr::String) =
    Pick(c, Symbol(attr))

abstract type SQLValue end

mutable struct FunSelect <: SQLClause
    select
    from
    where_
    group
    having
    order
end

mutable struct Unit <: SQLClause
end

mutable struct From <: SQLClause
    tbl::Table
end

mutable struct Select <: SQLClause
    base::SQLClause
    list::Vector{Pair{Symbol,SQLValue}}

    Select(base; pairs...) =
        new(base, collect(Pair{Symbol,SQLValue}, pairs))
end

mutable struct Where <: SQLClause
    base::SQLClause
    pred::SQLValue
end

mutable struct Join <: SQLClause
    left::SQLClause
    right::SQLClause
    on::SQLValue
end

struct Pick <: SQLValue
    alias::SQLClause
    field::Symbol
end

struct Op <: SQLValue
    op::Symbol
    args::Vector{SQLValue}

    Op(op, args...) =
        new(op, collect(SQLValue, args))
end

struct Const <: SQLValue
    val
end

function collect_refs!(v::Pick, refs)
    push!(refs, v)
    nothing
end

function collect_refs!(v::Op, refs)
    for arg in v.args
        collect_refs!(arg, refs)
    end
end

function collect_refs(@nospecialize v)
    refs = Set{SQLValue}()
    collect_refs!(v, refs)
    refs
end

collect_refs!(::Const, refs) =
    nothing

function collect_refs!(list::Vector{Pair{Symbol,SQLValue}}, refs)
    for (alias, v) in list
        collect_refs!(v, refs)
    end
end

replace_refs(v::Pick, repl) =
    get(repl, v, v)

replace_refs(v::Op, repl) =
    Op(v.op, SQLValue[replace_refs(arg, repl) for arg in v.args]...)

replace_refs(v::Const, repl) =
    v

replace_refs(list::Vector{Pair{Symbol,SQLValue}}, repl) =
    Pair{Symbol,SQLValue}[alias => replace_refs(v, repl) for (alias, v) in list]

default_list(@nospecialize ::SQLClause) = SQLValue[]

default_list(c::From) =
    SQLValue[Pick(c, col) for col in getfield(c, :tbl).cols]

default_list(c::Select) =
    SQLValue[Pick(c, alias) for (alias, v) in getfield(c, :list)]

default_list(c::Where) =
    default_list(getfield(c, :base))

default_list(c::Join) =
    vcat(default_list(getfield(c, :left)), default_list(getfield(c, :right)))

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
    c′ = Select(base′; list′...)
    repl = Dict{SQLValue,SQLValue}()
    for key in keys(refs)
        if key isa Pick && key.alias === c
            repl[key] = Pick(c′, key.field)
        end
    end
    c′, repl
end

function normalize(c::Where, refs)
    base = getfield(c, :base)
    pred = getfield(c, :pred)
    base_refs = collect_refs(pred)
    for item in refs
        push!(base_refs, item)
    end
    base′, base_repl = normalize(base, base_refs)
    pred′ = replace_refs(pred, base_repl)
    c′ = Where(base′, pred′)
    s = Select(c′)
    list = getfield(s, :list)
    repl = Dict{SQLValue,SQLValue}()
    pos = 0
    for key in refs
        if key in keys(base_repl)
            pos += 1
            field = Symbol("_", pos)
            repl[key] = Pick(s, field)
            push!(list, field => base_repl[key])
        end
    end
    s, repl
end

function normalize(c::Join, refs)
    left = getfield(c, :left)
    right = getfield(c, :right)
    on = getfield(c, :on)
    all_refs = collect_refs(on)
    for item in refs
        push!(all_refs, item)
    end
    left′, left_repl = normalize(left, all_refs)
    right′, right_repl = normalize(right, all_refs)
    all_repl = merge(left_repl, right_repl)
    on′ = replace_refs(on, all_repl)
    c′ = Join(left′, right′, on′)
    s = Select(c′)
    list = getfield(s, :list)
    repl = Dict{SQLValue,SQLValue}()
    pos = 0
    for key in refs
        if key in keys(all_repl)
            pos += 1
            field = Symbol("_", pos)
            repl[key] = Pick(s, field)
            push!(list, field => all_repl[key])
        end
    end
    s, repl
end

function normalize(c::From, refs)
    s = Select(c)
    list = getfield(s, :list)
    repl = Dict{SQLValue,SQLValue}()
    for key in refs
        if key isa Pick && key.alias === c
            field = key.field
            repl[key] = Pick(s, field)
            push!(list, field => Const(field))
        end
    end
    s, repl
end

to_sql!(ctx, v::Const) =
    to_sql!(ctx, v.val)

to_sql!(ctx, v::Pick) =
    to_sql!(ctx, (ctx.aliases[v.alias], v.field))

function to_sql!(ctx, v::Op)
    if v.op === :(=) && length(v.args) == 2
        print(ctx, "(")
        to_sql!(ctx, v.args[1])
        print(ctx, " = ")
        to_sql!(ctx, v.args[2])
        print(ctx, ")")
    elseif v.op === :(&&)
        print(ctx, '(')
        if isempty(v.args)
            print(ctx, "TRUE")
        else
            first = true
            for arg in v.args
                if !first
                    print(ctx, " AND ")
                else
                    first = false
                end
                to_sql!(ctx, arg)
            end
        end
        print(ctx, ')')
    elseif v.op === :placeholder && length(v.args) == 1
        print(ctx, '$')
        to_sql!(ctx, v.args[1])
    else
        error("unknown operation $(v.op)")
    end
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

