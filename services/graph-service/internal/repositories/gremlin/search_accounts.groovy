import org.janusgraph.core.attribute.Text

g.V().has('account', 'account_name', Text.textContainsPrefix(prefix))
    .limit(20)
    .project('account_id', 'name').by('account_id').by('account_name')
    .toList()
