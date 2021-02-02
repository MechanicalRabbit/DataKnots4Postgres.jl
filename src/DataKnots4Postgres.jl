
module DataKnots4Postgres

import DataKnots:
    AbstractShape,
    BlockOf,
    BlockVector,
    Cardinality,
    DataKnot,
    DataNode,
    DataShape,
    HasSlots,
    IsLabeled,
    Pipeline,
    Signature,
    SlotShape,
    Runtime,
    TableCell,
    TupleOf,
    TupleVector,
    ValueOf,
    as_tuples,
    backward_pass,
    block_cardinality,
    branch,
    chain_of,
    column,
    cover,
    deannotate,
    designate,
    elements,
    extract_branch,
    fill_node,
    filler,
    fits,
    flatten,
    forward_pass,
    get_root,
    head_node,
    join_node,
    lookup,
    part_node,
    pipe_node,
    print_expr,
    quoteof,
    quoteof_inner,
    render_cell,
    replace_branch,
    rewrite!,
    rewrite_dedup!,
    rewrite_passes,
    rewrite_simplify!,
    signature,
    shapeof,
    slot_node,
    source,
    syntaxof,
    target,
    tuple_lift,
    tuple_of,
    width,
    with_branch,
    with_elements,
    wrap,
    x0to1,
    x1to1,
    x1toN,
    @match_node

using Tables

using LibPQ:
    _DEFAULT_TYPE_MAP,
    PQ_SYSTEM_TYPES,
    Connection,
    execute

using PostgresCatalog:
    PGCatalog,
    PGColumn,
    PGForeignKey,
    PGSchema,
    PGTable,
    PGType,
    PGUniqueKey,
    introspect

import Base: show

struct Options
end

struct Handle{E}
    conn::Connection
    ety::E
    opt::Options
end

Handle(conn, ety::E) where {E} =
    Handle{E}(conn, ety, Options())

Handle(conn) =
    Handle(conn, introspect(conn))

struct VectorWithHandle{H,V,T} <: AbstractVector{T}
    h::H
    v::V
end

VectorWithHandle(h::H, v::V) where {H,T,V<:AbstractVector{T}} =
    VectorWithHandle{H,V,T}(h, v)

handle(hv::VectorWithHandle) =
    hv.h

output(hv::VectorWithHandle) =
    hv.v

Base.getindex(hv::VectorWithHandle) =
    hv.v

Base.getindex(hv::VectorWithHandle, k::Integer) =
    hv.v[k]

Base.getindex(hv::VectorWithHandle, ks::AbstractVector) =
    VectorWithHandle(hv.h, hv.v[ks])

Base.size(hv::VectorWithHandle) =
    size(hv.v)

Base.eltype(hv::VectorWithHandle) =
    eltype(hv.v)

Base.IndexStyle(::Type{<:VectorWithHandle{H,V}}) where {H,V<:AbstractVector} =
    IndexStyle(V)

struct EntityShape{E} <: DataShape
    ety::E
    opt::Options
    out::AbstractShape
end

entity(shp::EntityShape) = shp.ety

options(shp::EntityShape) = shp.opt

output(shp::EntityShape) = shp.out

replace_output(shp::EntityShape, f) =
    EntityShape(shp.ety, shp.opt, f isa AbstractShape ? f : f(shp.out))

width(::EntityShape) = 1

branch(shp::EntityShape, j) =
    (checkbounds(1:1, j); shp.out)

function replace_branch(shp::EntityShape, j::Int, f)
    checkbounds(1:1, j)
    replace_output(shp, f)
end

Base.eltype(shp::EntityShape) =
    eltype(shp.out)

quoteof(shp::EntityShape) =
    Expr(:call, nameof(EntityShape), quoteof_inner(shp.ety), quoteof_inner(shp.opt), quoteof_inner(shp.out))

syntaxof(shp::EntityShape) =
    Symbol(string(shp.ety))

shapeof(hv::VectorWithHandle) =
    EntityShape(hv.h.ety, hv.h.opt, shapeof(hv.v))

fits(shp1::EntityShape, shp2::EntityShape) =
    shp1.ety == shp2.ety && shp1.opt == shp2.opt && fits(shp1.out, shp2.out)

DataKnot(conn::Connection) =
    DataKnot(Any, VectorWithHandle(Handle(conn), TupleVector(1)), x1to1)

render_cell(shp::EntityShape, vals::VectorWithHandle, idx::Int, avail::Int, depth::Int=0) =
    if fits(output(shp), TupleOf())
        TableCell(string(entity(shp)))
    else
        render_cell(output(shp), output(vals), idx, avail, depth)
    end

output() = Pipeline(output)

output(::Runtime, input) =
    output(input)

function output(rt::Runtime, @nospecialize(shp::AbstractShape))
    @assert shp isa EntityShape
    Signature(EntityShape(entity(shp), options(shp), SlotShape()) |> HasSlots,
              SlotShape(),
              1, [1])
end

function extract_branch(::EntityShape, j)
    @assert j == 1
    output()
end

with_output(p) = Pipeline(with_output, p)

function with_output(rt::Runtime, input::AbstractVector, p)
    @assert input isa VectorWithHandle
    VectorWithHandle(handle(input), p(rt, output(input)))
end

function with_output(rt::Runtime, src::AbstractShape, p)
    @assert src isa EntityShape
    sig = p(rt, output(src))
    return with_elements(sig, entity(src), options(src))
end

function with_output(sig::Signature, ety, opt)
    src′ = EntityShape(ety, opt, source(sig))
    tgt′ = BlockOf(ety, opt, target(sig))
    bds′ = bindings(sig)
    if bds′ !== nothing
        src′ = HasSlots(src′, bds′.src_ary)
        if bds′.tgt_ary > 0
            tgt′ = HasSlots(tgt′, bds′.tgt_ary)
        end
    end
    Signature(src′, tgt′, bds′)
end

function with_branch(::EntityShape, j, p)
    @assert j == 1
    with_output(p)
end

postgres_name(name::AbstractString) =
    "\"$(replace(name, "\"" => "\"\""))\""

postgres_name(qname::Tuple) =
    join(postgres_name.(qname), '.')

postgres_name(names::AbstractVector) =
    join(postgres_name.(names), ", ")

postgres_name(name::Integer) =
    "\$$name"

postgres_table(tbl_name, col_names, col_types) =
    Pipeline(postgres_table, tbl_name, col_names, col_types)

postgres_table(tbl_name, col_names, col_types, icol_names) =
    Pipeline(postgres_table, tbl_name, col_names, col_types, icol_names)

function postgres_table(::Runtime, input::AbstractVector, tbl_name, col_names, col_types, icol_names=String[])
    @assert input isa VectorWithHandle
    sql = "SELECT $(postgres_name(col_names)) FROM $(postgres_name(tbl_name))"
    if !isempty(icol_names)
        condition = join(["$(postgres_name(icol_name)) = $(postgres_name(i))"
                          for (i, icol_name) in enumerate(icol_names)], " AND ")
        sql = "$sql WHERE $condition"
    end
    results = []
    for row in input
        res = execute(handle(input).conn, sql, values(row))
        push!(results, (length(res), columntable(res)))
    end
    offs = Vector{Int}(undef, length(results)+1)
    offs[1] = top = 1
    lbls = Symbol.(col_names)
    cols = AbstractVector[col_type[] for col_type in col_types]
    for k = eachindex(results)
        sz, res = results[k]
        top += sz
        offs[k+1] = top
        for j = 1:length(cols)
            append!(cols[j], res[j])
        end
    end
    tv = TupleVector(lbls, top-1, cols)
    h = handle(input)
    ety = h.ety
    ety′ = get_catalog(ety)[tbl_name[1]][tbl_name[2]]
    h′ = Handle(h.conn, ety′, h.opt)
    hv = VectorWithHandle(h′, tv)
    BlockVector(offs, hv)
end

function postgres_table(::Runtime, src::AbstractShape, tbl_name, col_names, col_types, icol_names=String[])
    @assert src isa EntityShape
    ety = entity(src)
    opt = options(src)
    out = output(src)
    @assert out isa TupleOf && width(out) == length(icol_names)
    ety′ = get_catalog(ety)[tbl_name[1]][tbl_name[2]]
    out′ = TupleOf(Symbol[Symbol(col_name) for col_name in col_names],
                   AbstractShape[ValueOf(col_type) for col_type in col_types])
    Signature(src, BlockOf(EntityShape(ety′, opt, out′)))
end

postgres_query(sql, col_types) =
    Pipeline(postgres_query, sql, col_types)

function postgres_query(::Runtime, input::AbstractVector, sql, col_types)
    @assert input isa VectorWithHandle
    results = []
    for row in input
        res = execute(handle(input).conn, sql, values(row))
        push!(results, (length(res), columntable(res)))
    end
    offs = Vector{Int}(undef, length(results)+1)
    offs[1] = top = 1
    cols = AbstractVector[col_type[] for col_type in col_types]
    for k = eachindex(results)
        sz, res = results[k]
        top += sz
        offs[k+1] = top
        for j = 1:length(cols)
            append!(cols[j], res[j])
        end
    end
    tv = TupleVector(Symbol[], top-1, cols)
    h = handle(input)
    hv = VectorWithHandle(h, tv)
    BlockVector(offs, hv)
end

function postgres_query(::Runtime, src::AbstractShape, sql, col_types)
    @assert src isa EntityShape
    ety = entity(src)
    opt = options(src)
    out = output(src)
    out′ = TupleOf(Symbol[],
                   AbstractShape[ValueOf(col_type) for col_type in col_types])
    Signature(src, BlockOf(EntityShape(ety, opt, out′)))
end

postgres_entity(tbl_name) =
    Pipeline(postgres_entity, tbl_name)

function postgres_entity(::Runtime, input::AbstractVector, tbl_name)
    @assert input isa VectorWithHandle
    h = handle(input)
    ety = h.ety
    ety′ = get_catalog(ety)[tbl_name[1]][tbl_name[2]]
    h′ = Handle(h.conn, ety′, h.opt)
    VectorWithHandle(h′, output(input))
end

function postgres_entity(::Runtime, src::AbstractShape, tbl_name)
    @assert src isa EntityShape
    ety = entity(src)
    opt = options(src)
    out = output(src)
    ety′ = get_catalog(ety)[tbl_name[1]][tbl_name[2]]
    Signature(EntityShape(ety, opt, SlotShape()) |> HasSlots(),
              EntityShape(ety′, opt, SlotShape()) |> HasSlots(),
              1, [1])
end

get_catalog(cat::PGCatalog) = cat

get_catalog(tbl::PGTable) = tbl.schema.catalog

function get_type(col::PGColumn)
    T = get_type(col.type)
    if !col.not_null
        T = Union{T, Missing}
    end
    T
end

function get_type(typ::PGType)
    key = Symbol(typ.name)
    if typ.schema.name == "pg_catalog"
        key = Symbol(typ.name)
        if haskey(PQ_SYSTEM_TYPES, key)
            key_oid = PQ_SYSTEM_TYPES[key]
            if haskey(_DEFAULT_TYPE_MAP, key_oid)
                return _DEFAULT_TYPE_MAP[key_oid]
            end
        end
    end
    String
end

guess_name(fk::PGForeignKey) =
    fk.name

guess_referring_name(fk::PGForeignKey) =
    "$(fk.table.name)_via_$(fk.name)"

function lookup(src::EntityShape{PGCatalog}, name::Symbol)
    p = lookup(EntityShape(src.ety["public"], src.opt, src.out), name)
    p !== nothing || return p
    p |> designate(src, target(p))
end

function lookup(src::EntityShape{PGSchema}, name::Symbol)
    scm = src.ety
    tbl = get(scm, string(name), nothing)
    tbl !== nothing || return nothing
    @assert tbl.primary_key !== nothing
    tbl_name = (tbl.schema.name, tbl.name)
    col_names = [col.name for col in tbl.primary_key.columns]
    col_types = Type[get_type(col) for col in tbl.primary_key.columns]
    p = postgres_table(tbl_name, col_names, col_types)
    tgt = EntityShape(tbl, src.opt, TupleOf(Symbol.(col_names), ValueOf.(col_types))) |> IsLabeled(name) |> BlockOf
    p |> designate(src, tgt)
end

function lookup(src::EntityShape{PGTable}, name::Symbol)
    tbl = src.ety
    for col in tbl
        if col.name == string(name)
            col_type = get_type(col)
            tbl_name = (tbl.schema.name, tbl.name)
            col_names = [col.name]
            col_types = Type[col_type]
            icol_names = [col.name for col in tbl.primary_key.columns]
            c = cover(ValueOf(col_type) |> IsLabeled(name))
            return chain_of(
                    postgres_table(tbl_name, col_names, col_types, icol_names),
                    block_cardinality(x1to1),
                    with_elements(chain_of(output(), column(1), c)),
                    flatten(),
            ) |> designate(src, target(c))
        end
    end
    for fk in tbl.foreign_keys
        if guess_name(fk) == string(name)
            ttbl = fk.target_table
            @assert ttbl.primary_key !== nothing
            tbl_name = (tbl.schema.name, tbl.name)
            col_names = [col.name for col in fk.columns]
            col_types = Type[get_type(col) for col in fk.columns]
            icol_names = [col.name for col in tbl.primary_key.columns]
            p0 = postgres_table(tbl_name, col_names, col_types, icol_names)
            tbl_name = (ttbl.schema.name, ttbl.name)
            col_names = [col.name for col in ttbl.primary_key.columns]
            col_types = Type[get_type(col) for col in ttbl.primary_key.columns]
            icol_names = [col.name for col in fk.target_columns]
            p1 = postgres_table(tbl_name, col_names, col_types, icol_names)
            card = x1to1
            if any(col -> !col.not_null, fk.columns)
                card |= x0to1
            end
            tgt = BlockOf(EntityShape(ttbl, src.opt, TupleOf(Symbol.(col_names), ValueOf.(col_types))) |> IsLabeled(name), card)
            return chain_of(
                    p0,
                    block_cardinality(x1to1),
                    with_elements(chain_of(p1, block_cardinality(card))),
                    flatten(),
            ) |> designate(src, tgt)
        end
    end
    for fk in tbl.referring_foreign_keys
        if guess_referring_name(fk) == string(name)
            ttbl = fk.table
            @assert ttbl.primary_key !== nothing
            tbl_name = (tbl.schema.name, tbl.name)
            col_names = [col.name for col in fk.target_columns]
            col_types = Type[get_type(col) for col in fk.target_columns]
            icol_names = [col.name for col in tbl.primary_key.columns]
            p0 = postgres_table(tbl_name, col_names, col_types, icol_names)
            tbl_name = (ttbl.schema.name, ttbl.name)
            col_names = [col.name for col in ttbl.primary_key.columns]
            col_types = Type[get_type(col) for col in ttbl.primary_key.columns]
            icol_names = [col.name for col in fk.columns]
            p1 = postgres_table(tbl_name, col_names, col_types, icol_names)
            card = x0to1
            if !any(uk -> uk.columns == fk.columns, ttbl.unique_keys)
                card |= x1toN
            end
            tgt = BlockOf(EntityShape(ttbl, src.opt, TupleOf(Symbol.(col_names), ValueOf.(col_types))) |> IsLabeled(name), card)
            return chain_of(
                    p0,
                    block_cardinality(x1to1),
                    with_elements(chain_of(p1, block_cardinality(card))),
                    flatten(),
            ) |> designate(src, tgt)
        end
    end
    nothing
end

lookup(src::EntityShape, name::Symbol) =
    error("not implemented")

rewrite_passes(::Val{(:DataKnots4Postgres,)}) =
    Pair{Int,Function}[
        100 => rewrite_pushdown!,
        110 => rewrite_simplify!,
        120 => rewrite_dedup!,
    ]

function rewrite_simplify_output!(node::DataNode)
    forward_pass(node) do n
        @match_node begin
            if (n ~ fill_node(pipe_node(output(), head_node(base)), part ~ part_node(base′, _))) && base === base′
                return rewrite!(n => part)
            end
        end
    end
end

function column_node(base, j)
    head = head_node(base)
    part = part_node(base, j)
    w = width(deannotate(head.shp))
    sig = Signature(head.shp, SlotShape(), w, [j])
    fill_node(pipe_node(column(j) |> designate(sig), head), part)
end

function output_node(base)
    head = head_node(base)
    part = part_node(base, 1)
    w = width(deannotate(head.shp))
    sig = Signature(head.shp, SlotShape(), 1, [1])
    fill_node(pipe_node(output() |> designate(sig), head), part)
end

struct SQLExpr
    head::Symbol
    args::Vector{Any}

    SQLExpr(head, args...) =
        new(head, collect(Any, args))
end

show(io::IO, e::SQLExpr) =
    print_expr(io, quoteof(e))

quoteof(e::SQLExpr) =
    Expr(:call, nameof(SQLExpr), QuoteNode(e.head), Any[quoteof(arg) for arg in e.args]...)

mutable struct ToSQLContext <: IO
    io::IOBuffer
    need_select::Union{Bool,Nothing}

    ToSQLContext() =
        new(IOBuffer(), nothing)
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

to_sql!(ctx, e::SQLExpr) =
    to_sql!(ctx, Val(e.head), e.args)

function to_sql!(ctx, ::Val{:call}, args)
    if length(args) == 3 && args[1] == :(=)
        print(ctx, '(')
        to_sql!(ctx, args[2])
        print(ctx, ' ', args[1], ' ')
        to_sql!(ctx, args[3])
        print(ctx, ')')
    elseif length(args) >= 1 && args[1] isa Symbol
        print(ctx, args[1], '(')
        first = true
        for arg in args[2:end]
            if first
                first = false
            else
                print(ctx, ", ")
            end
            to_sql!(ctx, arg)
        end
        print(ctx, ')')
    else
        error()
    end
end

function to_sql!(ctx, ::Val{:(&&)}, args)
    print(ctx, '(')
    if isempty(args)
        print(ctx, "TRUE")
    else
        first = true
        for arg in args
            if !first
                print(ctx, " AND ")
            else
                first = false
            end
            to_sql!(ctx, arg)
        end
    end
    print(ctx, ')')
end

function to_sql!(ctx, ::Val{:slot}, args)
    if ctx.need_select === false
        return
    end
    print(ctx, "SELECT /* slot */")
end

function to_sql!(ctx, ::Val{:placeholder}, args)
    k = args[1]
    print(ctx, postgres_name(k))
end

function to_sql!(ctx, ::Val{:select}, args)
    if ctx.need_select === false
        return
    end
    print(ctx, "SELECT")
    first = true
    for arg in args
        if first
            print(ctx, ' ')
            first = false
        else
            print(ctx, ", ")
        end
        to_sql!(ctx, arg)
    end
end

function ensure_select!(f, ctx, tail)
    need_select = ctx.need_select
    if need_select === nothing
        ctx.need_select = true
        to_sql!(ctx, tail)
        ctx.need_select = false
        f()
        ctx.need_select = nothing
    elseif need_select
        to_sql!(ctx, tail)
    else
        f()
    end
    nothing
end

function to_sql!(ctx, ::Val{:from}, args)
    if length(args) == 3
        alias, tbl, tail = args
        ensure_select!(ctx, tail) do
            print(ctx, " FROM ")
            to_sql!(ctx, tbl)
            print(ctx, " AS ")
            to_sql!(ctx, alias)
            to_sql!(ctx, tail)
        end
        return
    end
    error()
end

function to_sql!(ctx, ::Val{:join}, args)
    if length(args) == 5
        kind, alias, tbl, pred, tail = args
        ensure_select!(ctx, tail) do
            print(ctx, ' ', uppercase(string(kind)), " JOIN ")
            to_sql!(ctx, tbl)
            print(ctx, " AS ")
            to_sql!(ctx, alias)
            if kind !== :cross
                print(ctx, " ON ")
                to_sql!(ctx, pred)
            end
            to_sql!(ctx, tail)
        end
        return
    end
    error()
end

function to_sql!(ctx, ::Val{:where}, args)
    if length(args) == 2
        pred, tail = args
        ensure_select!(ctx, tail) do
            print(ctx, " WHERE ")
            to_sql!(ctx, pred)
            to_sql!(ctx, tail)
        end
        return
    end
    error()
end

mutable struct SQLAlias
    tbl::PGTable
    parent::Union{SQLAlias,Nothing}
    children::Vector{SQLAlias}
    on::Union{PGUniqueKey,PGForeignKey,Vector{PGColumn},Nothing}
    name::Symbol

    SQLAlias(tbl) =
        new(tbl, nothing, SQLAlias[], nothing, gensym())

    SQLAlias(tbl, on) =
        new(tbl, nothing, SQLAlias[], on, gensym())

    function SQLAlias(tbl, parent, on)
        a = new(tbl, parent, SQLAlias[], on, gensym())
        push!(parent.children, a)
        a
    end
end

abstract type AbstractTranslation end

mutable struct LoadTable <: AbstractTranslation
    alias::SQLAlias
    cols::Vector{PGColumn}
end

mutable struct HeadOfRootLoadTable <: AbstractTranslation
    base::LoadTable
end

mutable struct CardOfNestedLoadTable <: AbstractTranslation
    base::LoadTable
end

mutable struct PartOfLoadTable <: AbstractTranslation
    base::LoadTable
end

mutable struct SlotOfPartOfLoadTable <: AbstractTranslation
    base::PartOfLoadTable
end

mutable struct OutputOfPartOfLoadTable <: AbstractTranslation
    base::PartOfLoadTable
end

mutable struct ColumnOfOutputOfPartOfLoadTable <: AbstractTranslation
    base::OutputOfPartOfLoadTable
    k::Int
end

mutable struct SQLBundle
    top::DataNode
    root::SQLAlias
    trs::Dict{DataNode,AbstractTranslation}
    front::Vector{DataNode}

    SQLBundle(top, root) =
        new(top, root, Dict{DataNode,AbstractTranslation}(), DataNode[])
end


function rewrite_pushdown!(node::DataNode)
    bundles = SQLBundle[]
    bundle_by_node = Dict{DataNode,SQLBundle}()
    forward_pass(node) do n
        @match_node if (n ~ pipe_node(p ~ postgres_table(table_name::Tuple{String,String}, String[col_name], Type[col_type]), input))
            ishp = input.shp::EntityShape{PGCatalog}
            tbl = ishp.ety[table_name[1]][table_name[2]]
            col = tbl[col_name]
            root = SQLAlias(tbl)
            bundle = SQLBundle(n, root)
            push!(bundles, bundle)
            bundle_by_node[n] = bundle
            bundle.trs[n] = LoadTable(root, [col])
        elseif (n ~ pipe_node(p ~ postgres_table(table_name::Tuple{String,String}, String[col_name], Type[col_type], String[icol_name]), input))
            bundle = get(bundle_by_node, input, nothing)
            bundle !== nothing || return
            base_tr = bundle.trs[input]
            base_tr isa PartOfLoadTable || return
            parent_alias = base_tr.base.alias
            parent_tbl = parent_alias.tbl
            parent_cols = base_tr.base.cols
            joined_tbl = get_catalog(parent_tbl)[table_name[1]][table_name[2]]
            joined_cols = [joined_tbl[icol_name]]
            is_1to1 = false
            on = nothing
            if parent_tbl === joined_tbl && parent_tbl.primary_key !== nothing && parent_tbl.primary_key.columns == parent_cols == joined_cols
                is_1to1 = true
                on = parent_tbl.primary_key
            elseif all(col.not_null for col in parent_cols)
                for fk in parent_tbl.foreign_keys
                    if fk.columns == parent_cols && fk.target_table === joined_tbl && fk.target_columns == joined_cols
                        is_1to1 = true
                        on = fk
                        break
                    end
                end
            end
            if is_1to1
                bundle_by_node[n] = bundle
                alias = SQLAlias(joined_tbl, parent_alias, on)
                bundle.trs[n] = LoadTable(alias, [joined_tbl[col_name]])
            else
                on = [joined_tbl[icol_name]]
                root = SQLAlias(joined_tbl, on)
                bundle = SQLBundle(n, root)
                push!(bundles, bundle)
                bundle_by_node[n] = bundle
                bundle.trs[n] = LoadTable(root, [joined_tbl[col_name]])
            end
        elseif (n ~ head_node(base))
            bundle = get(bundle_by_node, base, nothing)
            bundle !== nothing || return
            base_tr = bundle.trs[base]
            base_tr isa LoadTable && base_tr.alias.parent === nothing || return
            bundle_by_node[n] = bundle
            bundle.trs[n] = HeadOfRootLoadTable(base_tr)
        elseif (n ~ pipe_node(block_cardinality(card), head_node(base))) && card == x1to1
            bundle = get(bundle_by_node, base, nothing)
            bundle !== nothing || return
            base_tr = bundle.trs[base]
            base_tr isa LoadTable && base_tr.alias.parent !== nothing || return
            bundle_by_node[n] = bundle
            bundle.trs[n] = CardOfNestedLoadTable(base_tr)
        elseif (n ~ part_node(base, _))
            bundle = get(bundle_by_node, base, nothing)
            bundle !== nothing || return
            base_tr = bundle.trs[base]
            base_tr isa LoadTable || return
            bundle_by_node[n] = bundle
            bundle.trs[n] = PartOfLoadTable(base_tr)
        elseif (n ~ slot_node(base))
            bundle = get(bundle_by_node, base, nothing)
            bundle !== nothing || return
            base_tr = bundle.trs[base]
            base_tr isa PartOfLoadTable || return
            bundle_by_node[n] = bundle
            bundle.trs[n] = SlotOfPartOfLoadTable(base_tr)
        elseif (n ~ fill_node(pipe_node(output(), head_node(base)), part_node(base′, _))) && base === base′
            bundle = get(bundle_by_node, base, nothing)
            bundle !== nothing || return
            base_tr = bundle.trs[base]
            base_tr isa PartOfLoadTable || return
            bundle_by_node[n] = bundle
            bundle.trs[n] = OutputOfPartOfLoadTable(base_tr)
        elseif (n ~ fill_node(pipe_node(column(k::Int), head_node(base)), part_node(base′, _))) && base === base′
            bundle = get(bundle_by_node, base, nothing)
            bundle !== nothing || return
            base_tr = bundle.trs[base]
            base_tr isa OutputOfPartOfLoadTable || return
            bundle_by_node[n] = bundle
            bundle.trs[n] = ColumnOfOutputOfPartOfLoadTable(base_tr, k)
        end
    end
    backward_pass(node) do n
        bundle = get(bundle_by_node, n, nothing)
        if bundle !== nothing
            is_front = false
            for (use, k) in n.uses
                if get(bundle_by_node, use, nothing) !== bundle
                    is_front = true
                end
            end
            if is_front
                push!(bundle.front, n)
            end
        else
            common_bundle = missing
            for (use, k) in n.uses
                use_bundle = get(bundle_by_node, use, nothing)
                if use_bundle !== nothing
                    if common_bundle === missing
                        common_bundle = use_bundle
                    elseif common_bundle !== use_bundle
                        common_bundle = nothing
                    end
                else
                    common_bundle = nothing
                end
            end
            if common_bundle !== missing && common_bundle !== nothing
                bundle_by_node[n] = common_bundle
            end
        end
    end
    for bundle in bundles
        ex, columns = make_sql(bundle)
        sql = to_sql(ex)
        col_types = Type[get_type(col) for (alias, col) in columns]
        query_p = postgres_query(sql, col_types)
        src = bundle.top.refs[1].shp
        ety = entity(src)
        opt = options(src)
        out = output(src)
        out′ = TupleOf(Symbol[],
                       AbstractShape[ValueOf(col_type) for col_type in col_types])
        sig = Signature(src, BlockOf(EntityShape(ety, opt, out′)))
        query_node = pipe_node(query_p |> designate(sig), bundle.top.refs[1])
        head_of_query_node = head_node(query_node)
        part_of_query_node = part_node(query_node, 1)
        output_of_query_node = output_node(part_of_query_node)
        repl = Pair{DataNode,DataNode}[]
        for n in bundle.front
            tr = bundle.trs[n]
            if tr isa HeadOfRootLoadTable
                n′ = head_of_query_node
            elseif tr isa CardOfNestedLoadTable
                sig′ = Signature(SlotShape(), BlockOf(SlotShape(), x1to1) |> HasSlots, 1, [1])
                p′ = wrap() |> designate(sig′)
                n′ = pipe_node(p′, slot_node(part_of_query_node))
            elseif tr isa PartOfLoadTable
                alias = tr.base.alias
                idxs = Int[findfirst(==((alias, col)), columns) for col in tr.base.cols]
                lbls′ = Symbol[Symbol(col.name) for col in tr.base.cols]
                head′ = head_node(part_of_query_node)
                part′ = part_node(part_of_query_node, 1)
                entity_sig′ = Signature(EntityShape(ety, opt, SlotShape()) |> HasSlots, EntityShape(alias.tbl, opt, SlotShape()) |> HasSlots, 1, [1])
                entity_p′ = postgres_entity((alias.tbl.schema.name, alias.tbl.name)) |> designate(entity_sig′)
                entity_node′ = pipe_node(entity_p′, head′)
                tup_sig′ = Signature(SlotShape(), TupleOf(lbls′, AbstractShape[SlotShape() for k in idxs]) |> HasSlots(length(idxs)), 1, fill(1, length(idxs)))
                tup_p′ = tuple_of(lbls′, length(idxs)) |> designate(tup_sig′)
                tup_node′ = pipe_node(tup_p′, slot_node(part′))
                n′ = join_node(entity_node′, [join_node(tup_node′, [column_node(part′, idx) for idx in idxs])])
            elseif tr isa SlotOfPartOfLoadTable
                n′ = slot_node(part_of_query_node)
            elseif tr isa ColumnOfOutputOfPartOfLoadTable
                alias = tr.base.base.base.alias
                col = tr.base.base.base.cols[tr.k]
                k = findfirst(==((alias, col)), columns)
                n′ = column_node(output_of_query_node, k)
            else
                error(typeof(tr))
            end
            push!(repl, n => n′)
        end
        rewrite!(repl)
    end
end

function make_columns(bundle)
    columns = Tuple{SQLAlias,PGColumn}[]
    for n in bundle.front
        tr = bundle.trs[n]
        k = nothing
        if tr isa PartOfLoadTable
            tr = tr.base
        elseif tr isa OutputOfPartOfLoadTable
            tr = tr.base.base
        elseif tr isa ColumnOfOutputOfPartOfLoadTable
            k = tr.k
            tr = tr.base.base.base
        end
        if tr isa LoadTable
            alias = tr.alias
            cols = tr.cols
            for (j, col) in enumerate(tr.cols)
                if k === nothing || k == j
                    push!(columns, (tr.alias, col))
                end
            end
        end
    end
    return columns
end

function make_joins(alias, ex)
    on = alias.on
    if alias.parent === nothing && on isa Vector{PGColumn}
        args = SQLExpr[]
        for (k, col) in enumerate(on)
            push!(args, SQLExpr(:call, :(=), (alias.name, Symbol(col.name)), SQLExpr(:placeholder, k)))
        end
        ex = SQLExpr(:where, SQLExpr(:(&&), args...), ex)
    end
    for child in alias.children
        ex = make_joins(child, ex)
    end
    if alias.parent === nothing
        ex = SQLExpr(:from, alias.name, (Symbol(alias.tbl.schema.name), Symbol(alias.tbl.name)), ex)
    elseif on isa PGUniqueKey
        args = SQLExpr[]
        for col in on.columns
            push!(args, SQLExpr(:call, :(=), (alias.parent.name, Symbol(col.name)), (alias.name, Symbol(col.name))))
        end
        ex = SQLExpr(:join, :inner, alias.name, (Symbol(alias.tbl.schema.name), Symbol(alias.tbl.name)), SQLExpr(:(&&), args...), ex)
    elseif on isa PGForeignKey
        args = SQLExpr[]
        for (col1, col2) in zip(on.columns, on.target_columns)
            push!(args, SQLExpr(:call, :(=), (alias.parent.name, Symbol(col1.name)), (alias.name, Symbol(col2.name))))
        end
        ex = SQLExpr(:join, :inner, alias.name, (Symbol(alias.tbl.schema.name), Symbol(alias.tbl.name)), SQLExpr(:(&&), args...), ex)
    end
    ex
end

function make_sql(bundle)
    columns = make_columns(bundle)
    select = SQLExpr(:select, [(alias.name, Symbol(col.name)) for (alias, col) in columns]...)
    ex = make_joins(bundle.root, select)
    ex, columns
end

end
