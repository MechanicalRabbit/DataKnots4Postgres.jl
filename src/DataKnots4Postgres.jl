module DataKnots4Postgres

using DataKnots
using LibPQ

DataKnots.DataKnot(conn::LibPQ.Connection) = error("not implemented")

end
