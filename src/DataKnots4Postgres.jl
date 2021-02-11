
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
    dissect,
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
    @dissect

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

include("funsql.jl")

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

dissect(mod::Module, scr::Symbol, ::typeof(output), ::Tuple{}) =
    dissect(mod, scr, Pipeline, (output, :([])))

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

function dissect(mod::Module, scr::Symbol, ::typeof(postgres_table), @nospecialize pats::Tuple{Any,Any,Any})
    tbl_pat, col_pat, colt_pat = pats
    dissect(mod, scr, Pipeline, (postgres_table, :([$tbl_pat::$(Tuple{String,String}), $col_pat::$(Vector{String}), $colt_pat::$(Vector{Type})])))
end

function dissect(mod::Module, scr::Symbol, ::typeof(postgres_table), @nospecialize pats::Tuple{Any,Any,Any,Any})
    tbl_pat, col_pat, colt_pat, icol_pat = pats
    dissect(mod, scr, Pipeline, (postgres_table, :([$tbl_pat::$(Tuple{String,String}), $col_pat::$(Vector{String}), $colt_pat::$(Vector{Type}), $icol_pat::$(Vector{String})])))
end

function dissect(mod::Module, scr::Symbol, ::typeof(block_cardinality), @nospecialize pats::Tuple{Any})
    card_pat, = pats
    dissect(mod, scr, Pipeline, (block_cardinality, :([$card_pat::Cardinality])))
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
        @dissect begin
            if (n ~ fill_node(pipe_node(output(), head_node(base)), part := part_node(base′, _))) && base === base′
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

mutable struct SQLBundle
    top::DataNode
    root::SQLAlias
    front::Vector{DataNode}

    SQLBundle(top, root) =
        new(top, root, DataNode[])
end

struct SQLMemo
    bundle::SQLBundle
    alias::SQLAlias
    cols::Vector{PGColumn}
    kind::Symbol
    k::Int
end

function rewrite_pushdown!(node::DataNode)
    bundles = SQLBundle[]
    forward_pass(node) do n
        n.memo = nothing
        @dissect if (n ~ pipe_node(p := postgres_table(table_name, [col_name], [col_type]), input))
            ishp = input.shp::EntityShape{PGCatalog}
            tbl = ishp.ety[table_name[1]][table_name[2]]
            col = tbl[col_name]
            root = SQLAlias(tbl)
            bundle = SQLBundle(n, root)
            push!(bundles, bundle)
            n.memo = SQLMemo(bundle, root, [col], :table, 0)
        elseif (n ~ pipe_node(p := postgres_table(table_name, [col_name], [col_type], [icol_name]), input))
            base_tr = input.memo
            base_tr isa SQLMemo && base_tr.kind === :part_of_table || return
            parent_alias = base_tr.alias
            parent_tbl = parent_alias.tbl
            parent_cols = base_tr.cols
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
                alias = SQLAlias(joined_tbl, parent_alias, on)
                n.memo = SQLMemo(base_tr.bundle, alias, [joined_tbl[col_name]], :table, 0)
            else
                on = [joined_tbl[icol_name]]
                root = SQLAlias(joined_tbl, on)
                bundle = SQLBundle(n, root)
                push!(bundles, bundle)
                n.memo = SQLMemo(bundle, root, [joined_tbl[col_name]], :table, 0)
            end
        elseif (n ~ head_node(base))
            base_tr = base.memo
            base_tr isa SQLMemo && base_tr.kind === :table && base_tr.alias.parent === nothing || return
            n.memo = SQLMemo(base_tr.bundle, base_tr.alias, base_tr.cols, :head_of_root_table, 0)
        elseif (n ~ pipe_node(block_cardinality(card), head := head_node(base))) && card == x1to1
            base_tr = base.memo
            base_tr isa SQLMemo && base_tr.kind === :table && base_tr.alias.parent !== nothing || return
            n.memo = SQLMemo(base_tr.bundle, base_tr.alias, base_tr.cols, :card_of_nested_table, 0)
            head.memo = SQLMemo(base_tr.bundle, base_tr.alias, base_tr.cols, :other, 0)
        elseif (n ~ part_node(base, _))
            base_tr = base.memo
            base_tr isa SQLMemo && base_tr.kind === :table || return
            n.memo = SQLMemo(base_tr.bundle, base_tr.alias, base_tr.cols, :part_of_table, 0)
        elseif (n ~ slot_node(base))
            base_tr = base.memo
            base_tr isa SQLMemo && base_tr.kind === :part_of_table || return
            n.memo = SQLMemo(base_tr.bundle, base_tr.alias, base_tr.cols, :slot_of_part_of_table, 0)
        elseif (n ~ fill_node(slot := pipe_node(output(), head := head_node(base)), part := part_node(base′, _))) && base === base′
            base_tr = base.memo
            base_tr isa SQLMemo && base_tr.kind === :part_of_table || return
            n.memo = SQLMemo(base_tr.bundle, base_tr.alias, base_tr.cols, :output_of_part_of_table, 0)
            slot.memo = head.memo = part.memo = SQLMemo(base_tr.bundle, base_tr.alias, base_tr.cols, :other, 0)
        elseif (n ~ fill_node(slot := pipe_node(column(k::Int), head := head_node(base)), part := part_node(base′, _))) && base === base′
            base_tr = base.memo
            base_tr isa SQLMemo && base_tr.kind === :output_of_part_of_table || return
            n.memo = SQLMemo(base_tr.bundle, base_tr.alias, base_tr.cols, :column_of_output_of_part_of_table, k)
            slot.memo = head.memo = part.memo = SQLMemo(base_tr.bundle, base_tr.alias, base_tr.cols, :other, 0)
        end
    end
    backward_pass(node) do n
        tr = n.memo
        if tr isa SQLMemo
            bundle = tr.bundle
            is_front = false
            for (use, k) in n.uses
                use_tr = use.memo
                if !(use_tr isa SQLMemo && use_tr.bundle === bundle)
                    is_front = true
                    break
                end
            end
            if is_front
                push!(bundle.front, n)
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
            tr = n.memo::SQLMemo
            if tr.kind === :head_of_root_table
                n′ = head_of_query_node
            elseif tr.kind === :card_of_nested_table
                sig′ = Signature(SlotShape(), BlockOf(SlotShape(), x1to1) |> HasSlots, 1, [1])
                p′ = wrap() |> designate(sig′)
                n′ = pipe_node(p′, slot_node(part_of_query_node))
            elseif tr.kind === :part_of_table
                alias = tr.alias
                idxs = Int[findfirst(==((alias, col)), columns) for col in tr.cols]
                lbls′ = Symbol[Symbol(col.name) for col in tr.cols]
                head′ = head_node(part_of_query_node)
                part′ = part_node(part_of_query_node, 1)
                entity_sig′ = Signature(EntityShape(ety, opt, SlotShape()) |> HasSlots, EntityShape(alias.tbl, opt, SlotShape()) |> HasSlots, 1, [1])
                entity_p′ = postgres_entity((alias.tbl.schema.name, alias.tbl.name)) |> designate(entity_sig′)
                entity_node′ = pipe_node(entity_p′, head′)
                tup_sig′ = Signature(SlotShape(), TupleOf(lbls′, AbstractShape[SlotShape() for k in idxs]) |> HasSlots(length(idxs)), 1, fill(1, length(idxs)))
                tup_p′ = tuple_of(lbls′, length(idxs)) |> designate(tup_sig′)
                tup_node′ = pipe_node(tup_p′, slot_node(part′))
                n′ = join_node(entity_node′, [join_node(tup_node′, [column_node(part′, idx) for idx in idxs])])
            elseif tr.kind === :slot_of_part_of_table
                n′ = slot_node(part_of_query_node)
            elseif tr.kind === :column_of_output_of_part_of_table
                alias = tr.alias
                col = tr.cols[tr.k]
                k = findfirst(==((alias, col)), columns)
                n′ = column_node(output_of_query_node, k)
            else
                error(tr.kind)
            end
            push!(repl, n => n′)
        end
        rewrite!(repl)
    end
end

function make_columns(bundle)
    columns = Tuple{SQLAlias,PGColumn}[]
    for n in bundle.front
        tr = n.memo::SQLMemo
        tr.kind in (:part_of_table, :output_of_part_of_table, :column_of_output_of_part_of_table) || continue
        if tr.k != 0
            push!(columns, (tr.alias, tr.cols[tr.k]))
        else
            for col in tr.cols
                push!(columns, (tr.alias, col))
            end
        end
    end
    return columns
end

function make_joins(join, alias, alias_to_from)
    on = alias.on
    tbl = Table(alias.tbl)
    from = From(tbl)
    alias_to_from[alias] = from
    if alias.parent === nothing
        @assert join === nothing
        join = from
    elseif on isa PGUniqueKey
        parent_from = alias_to_from[alias.parent]
        args = SQLValue[]
        for col in on.columns
            push!(args, Op(:(=), pick(parent_from, Symbol(col.name)), pick(from, Symbol(col.name))))
        end
        pred = Op(:(&&), args...)
        join = Join(join, from, pred)
    elseif on isa PGForeignKey
        parent_from = alias_to_from[alias.parent]
        args = SQLValue[]
        for (col1, col2) in zip(on.columns, on.target_columns)
            push!(args, Op(:(=), pick(parent_from, Symbol(col1.name)), pick(from, Symbol(col2.name))))
        end
        pred = Op(:(&&), args...)
        join = Join(join, from, pred)
    end
    for child in alias.children
        join = make_joins(join, child, alias_to_from)
    end
    if alias.parent === nothing && on isa Vector{PGColumn}
        args = SQLValue[]
        for (k, col) in enumerate(on)
            push!(args, Op(:(=), pick(from, Symbol(col.name)), Op(:placeholder, Const(k))))
        end
        pred = Op(:(&&), args...)
        join = Where(join, pred)
    end
    join
end

function make_sql(bundle)
    columns = make_columns(bundle)
    alias_to_from = Dict{SQLAlias,SQLClause}()
    j = make_joins(nothing, bundle.root, alias_to_from)
    list = Pair{Symbol,SQLValue}[]
    for (alias, col) in columns
        push!(list, Symbol(col.name) => pick(alias_to_from[alias], Symbol(col.name)))
    end
    select = Select(j; list...)
    return normalize(select), columns
end

end
