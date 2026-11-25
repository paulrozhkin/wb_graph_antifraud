import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.*

def transferTime = new Date((timeMillis as Number).longValue())
def amount = (amountKopecks as Number).longValue()
def existing = g.E().has('transfer', 'transaction_id', transactionId).limit(2).toList()
if (!existing.isEmpty()) {
    def edge = existing[0]
    def sameTransfer = existing.size() == 1 &&
        edge.outVertex().value('account_id') == fromAccountId &&
        edge.inVertex().value('account_id') == toAccountId &&
        edge.value('time') == transferTime &&
        edge.value('amount_kopecks') == amount
    return sameTransfer ? 'replayed' : 'conflict'
}

def account = { id ->
    g.V().has('account', 'account_id', id).fold()
        .coalesce(unfold(), addV('account').property('account_id', id)).next()
}
def sender = account(fromAccountId)
def recipient = account(toAccountId)
sender.addEdge('transfer', recipient, 'transaction_id', transactionId,
    'time', transferTime, 'amount_kopecks', amount)
return 'created'
