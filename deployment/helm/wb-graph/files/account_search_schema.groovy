import org.apache.tinkerpop.gremlin.structure.Vertex
import org.janusgraph.core.schema.Mapping
import org.janusgraph.core.schema.SchemaStatus

graph.tx().rollback()
def management = graph.openManagement()
try {
    def index = management.getGraphIndex('accountsByName')
    def nameKey = management.getPropertyKey('account_name')
    if (index != null) {
        if (index.getIndexStatus(nameKey) != SchemaStatus.ENABLED) {
            throw new IllegalStateException('accountsByName is not enabled')
        }
        return 'accountsByName already enabled'
    }

    // Create the indexed property in the same transaction, before any names are stored.
    nameKey = management.makePropertyKey('account_name').dataType(String.class).make()
    def accountLabel = management.getVertexLabel('account') ?: management.makeVertexLabel('account').make()
    management.buildIndex('accountsByName', Vertex.class)
        .addKey(nameKey, Mapping.TEXT.asParameter()).indexOnly(accountLabel).buildMixedIndex('search')
    management.commit()
    return 'Created accountsByName'
} finally {
    if (management.isOpen()) {
        management.rollback()
    }
}
