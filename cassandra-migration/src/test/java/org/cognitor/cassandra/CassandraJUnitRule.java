package org.cognitor.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import org.cassandraunit.CQLDataLoader;
import org.cassandraunit.dataset.CQLDataSet;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.rules.ExternalResource;

import java.net.InetSocketAddress;
import java.util.Collections;

import static org.cassandraunit.utils.EmbeddedCassandraServerHelper.DEFAULT_CASSANDRA_YML_FILE;

/**
 * Cassandra Rule class that loads data from a specified point to initialize the database.
 * This rule also increases the default timeout in case of a slow CI system.
 * 
 * @author Patrick Kranz
 *
 */
public class CassandraJUnitRule extends ExternalResource {

    public static final String TEST_KEYSPACE = "tenant_data_test";
    public static final String DEFAULT_SCRIPT_LOCATION = "cassandraTestInit.cql";

    private static final long TIMEOUT = 60000L;
    private static final String LOCALHOST = "127.0.0.1";

    private CqlSession session;
    private final String ymlFileLocation;
    private final String dataSetLocation;

    public CassandraJUnitRule() {
        this(DEFAULT_SCRIPT_LOCATION, DEFAULT_CASSANDRA_YML_FILE);
    }

    public CassandraJUnitRule(String dataSetLocation, String ymlFileLocation) {
        this.ymlFileLocation = ymlFileLocation;
        this.dataSetLocation = dataSetLocation;
    }

    private void load() {
        CQLDataSet dataSet = new ClassPathCQLDataSet(dataSetLocation, TEST_KEYSPACE);
        CQLDataLoader dataLoader = new CQLDataLoader(session);
        dataLoader.load(dataSet);
    }

    @Override
    protected void before() throws Exception {
        EmbeddedCassandraServerHelper.startEmbeddedCassandra(ymlFileLocation, TIMEOUT);

        session = CqlSession.builder().addContactPoints(
                Collections.singleton(InetSocketAddress.createUnresolved(LOCALHOST, 9142))).build();

        load();
    }

    @Override
    protected void after() {
        EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
    }

    public CqlSession getSession() {
        return this.session;
    }
}
