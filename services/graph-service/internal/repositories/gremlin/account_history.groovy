import org.apache.tinkerpop.gremlin.process.traversal.Order
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.*

// Caller supplies the traversal source g and the accountId binding.
g.V().has('account', 'account_id', accountId)
    .bothE('transfer').dedup()
    .order().by('time', Order.desc).by('transaction_id', Order.asc)
    .limit(20)
    .project('transaction_id', 'from_account_id', 'to_account_id', 'time', 'amount_kopecks')
        .by('transaction_id')
        .by(outV().values('account_id'))
        .by(inV().values('account_id'))
        .by('time')
        .by('amount_kopecks')
    .toList()
