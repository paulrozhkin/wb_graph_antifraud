import java.time.Instant

// Initialize a new graph once. Existing data survives a JanusGraph restart.
if (g.V().limit(1).hasNext()) {
    return 'Skipped demo seed: graph already contains data'
}
def account = { id -> g.addV('account').property('account_id', id).next() }
def anton = account('anton')
def vera = account('vera')
def boris = account('boris')
def transfer = { id, from, to, utcTime, kopecks ->
    from.addEdge('transfer', to, 'transaction_id', id,
        'time', Date.from(Instant.parse(utcTime)), 'amount_kopecks', kopecks)
}
transfer('t1', anton, vera, '2026-11-26T10:00:00Z', 50000L)
transfer('t2', anton, vera, '2026-11-26T11:00:00Z', 50000L)
transfer('t3', vera, anton, '2026-11-26T11:30:00Z', 20000L)
transfer('t4', vera, vera, '2026-11-26T11:30:00Z', 10000L)
transfer('t5', anton, boris, '2026-11-26T12:00:00Z', 10000L)
g.E().count().next()
