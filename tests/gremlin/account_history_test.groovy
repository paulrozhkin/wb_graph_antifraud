import java.time.Instant
import org.janusgraph.core.JanusGraphFactory

def graph = JanusGraphFactory.build()
    .set('storage.backend', 'inmemory')
    .set('graph.unique-instance-id', 'history-test')
    .open()
try {
    def g = graph.traversal()
    def seed = new File('deployment/helm/wb-graph/files/seed_history.groovy')
    2.times {
        new GroovyShell(new Binding([g: g])).evaluate(seed)
        graph.tx().commit()
    }
    assert g.V().count().next() == 3
    assert g.E().count().next() == 5

    def query = new File('services/graph-service/internal/repositories/gremlin/account_history.groovy')
    def history = { id -> new GroovyShell(new Binding([g: g, accountId: id])).evaluate(query) }
    def rows = history('vera')
    assert rows*.transaction_id == ['t3', 't4', 't2', 't1']
    assert rows*.from_account_id == ['vera', 'vera', 'anton', 'anton']
    assert rows*.to_account_id == ['anton', 'vera', 'vera', 'vera']
    assert rows*.amount_kopecks == [20000L, 10000L, 50000L, 50000L]
    assert history('boris')*.transaction_id == ['t5']
    assert history('unknown').isEmpty()
    println "Vera: ${rows*.transaction_id}"

    def anton = g.V().has('account_id', 'anton').next()
    def vera = g.V().has('account_id', 'vera').next()
    (1..21).each { n ->
        anton.addEdge('transfer', vera, 'transaction_id', "older-${n}".toString(),
            'time', Date.from(Instant.parse('2026-11-25T12:00:00Z')), 'amount_kopecks', 100L)
    }
    graph.tx().commit()
    def limited = history('vera')
    assert limited.size() == 20
    assert limited.take(4) == rows
    println 'Account history checks passed'
} finally {
    graph.close()
}
