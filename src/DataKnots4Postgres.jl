
module DataKnots4Postgres

import DataKnots:
    AbstractShape,
    BlockOf,
    BlockVector,
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
    designate,
    elements,
    fits,
    flatten,
    head_node,
    lookup,
    merge_into!,
    part_node,
    pipe_node,
    quoteof,
    quoteof_inner,
    render_cell,
    replace_branch,
    rewrite_passes,
    signature,
    shapeof,
    slot_node,
    source,
    syntaxof,
    target,
    width,
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
    introspect

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

postgres_name(name::AbstractString) =
    "\"$(replace(name, "\"" => "\"\""))\""

postgres_name(qname::Tuple) =
    join(postgres_name.(qname), '.')

postgres_name(names::AbstractVector) =
    join(postgres_name.(names), ", ")

postgres_name(name::Integer) =
    "\$$name"

load_postgres_table(tbl_name, col_names, col_types) =
    Pipeline(load_postgres_table, tbl_name, col_names, col_types)

load_postgres_table(tbl_name, col_names, col_types, icol_names) =
    Pipeline(load_postgres_table, tbl_name, col_names, col_types, icol_names)

function load_postgres_table(::Runtime, input::AbstractVector, tbl_name, col_names, col_types, icol_names=String[])
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

function load_postgres_table(::Runtime, src::AbstractShape, tbl_name, col_names, col_types, icol_names=String[])
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
    p = load_postgres_table(tbl_name, col_names, col_types)
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
                    load_postgres_table(tbl_name, col_names, col_types, icol_names),
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
            p0 = load_postgres_table(tbl_name, col_names, col_types, icol_names)
            tbl_name = (ttbl.schema.name, ttbl.name)
            col_names = [col.name for col in ttbl.primary_key.columns]
            col_types = Type[get_type(col) for col in ttbl.primary_key.columns]
            icol_names = [col.name for col in fk.target_columns]
            p1 = load_postgres_table(tbl_name, col_names, col_types, icol_names)
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
            p0 = load_postgres_table(tbl_name, col_names, col_types, icol_names)
            tbl_name = (ttbl.schema.name, ttbl.name)
            col_names = [col.name for col in ttbl.primary_key.columns]
            col_types = Type[get_type(col) for col in ttbl.primary_key.columns]
            icol_names = [col.name for col in fk.columns]
            p1 = load_postgres_table(tbl_name, col_names, col_types, icol_names)
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
        2 => rewrite_pushdown!,
    ]

function rewrite_pushdown!(node::DataNode)
    backward_pass(node) do node
        @match_node if (node ~ pipe_node(p ~ load_postgres_table(table_name, String[col], Type[col_type]), input))
            other_cols = Tuple{String,Type}[]
            for (n1, idx1) in node.uses
                if (n1 ~ head_node(_))
                elseif (n1 ~ part_node(_, _))
                    for (n2, idx2) in n1.uses
                        if (n2 ~ pipe_node(p ~ load_postgres_table(n2_table_name, String[n2_col], Type[n2_col_type], String[n2_icol]), _))
                            if n2_table_name == table_name && n2_icol == col
                                push!(other_cols, (n2_col, n2_col_type))
                                for (n3, idx3) in n2.uses
                                    if (n3 ~ head_node(_))
                                        for (n4, idx4) in n3.uses
                                            if (n4 ~ pipe_node(block_cardinality(card), _)) && card == x1to1
                                            else
                                                return
                                            end
                                         end
                                    elseif (n3 ~ part_node(_, _))
                                    else
                                        return
                                    end
                                end
                            else
                                return
                            end
                        else
                            return
                        end
                    end
                else
                    return
                end
            end
            length(other_cols) == 1 || return
            other_col_name, other_col_type = other_cols[1]
            sig = signature(p)
            tgt = target(sig)
            out′ = TupleOf(Symbol[Symbol(other_col_name)], AbstractShape[ValueOf(other_col_type)])
            tgt′ = BlockOf(EntityShape(entity(elements(tgt)), options(elements(tgt)), out′))
            p′ = load_postgres_table(table_name, String[other_col_name], Type[other_col_type]) |> designate(source(sig), tgt′)
            node′ = pipe_node(p′, input)
            for (n1, idx1) in copy(node.uses)
                if (n1 ~ head_node(_))
                    n1′ = head_node(node′)
                    merge_into!(n1, n1′)
                elseif (n1 ~ part_node(_, _))
                    n1′ = part_node(node′, 1)
                    for (n2, idx2) in copy(n1.uses)
                        if (n2 ~ pipe_node(load_postgres_table(_, _, _, _), _))
                            for (n3, idx3) in copy(n2.uses)
                                if (n3 ~ head_node(_))
                                    for (n4, idx4) in n3.uses
                                        if (n4 ~ pipe_node(block_cardinality(card), _))
                                            sig4′ = Signature(SlotShape(),
                                                              BlockOf(SlotShape(), x1to1) |> HasSlots,
                                                              1, [1])
                                            p4′ = wrap() |> designate(sig4′)
                                            n4′ = pipe_node(p4′, slot_node(n1′))
                                            merge_into!(n4, n4′)
                                        else
                                            error()
                                        end
                                     end
                                elseif (n3 ~ part_node(_, _))
                                    merge_into!(n3, n1′)
                                else
                                    error()
                                end
                            end
                        else
                            error()
                        end
                    end
                else
                    error()
                end
            end
        end
    end
end

end
