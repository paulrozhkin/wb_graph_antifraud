import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.*

// Read edges first so that their endpoints can be collected in the same request.
def transfers = g.E().hasLabel('transfer').limit(transferScanLimit)
    .project('transaction_id', 'from_account_id', 'to_account_id', 'time', 'amount_kopecks')
        .by('transaction_id')
        .by(outV().values('account_id'))
        .by(inV().values('account_id'))
        .by('time')
        .by('amount_kopecks')
    .toList()

def accounts = g.V().hasLabel('account').limit(accountScanLimit)
    .project('account_id', 'name')
        .by('account_id')
        .by(coalesce(values('account_name'), values('account_id')))
    .toList()

[[accounts: accounts, transfers: transfers]]
