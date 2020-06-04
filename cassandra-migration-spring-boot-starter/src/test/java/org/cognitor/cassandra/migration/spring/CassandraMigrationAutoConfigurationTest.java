package org.cognitor.cassandra.migration.spring;


import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import org.cassandraunit.CQLDataLoader;
import org.cassandraunit.dataset.CQLDataSet;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.cognitor.cassandra.migration.MigrationTask;
import org.hamcrest.CoreMatchers;
import org.junit.Ignore;
import org.junit.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.springframework.boot.test.util.EnvironmentTestUtils.addEnvironment;

/**
 * @author Patrick Kranz
 */

@Ignore
public class CassandraMigrationAutoConfigurationTest {

    @Test
    public void shouldMigrateDatabaseWhenClusterGiven() {
        AnnotationConfigApplicationContext context =
                new AnnotationConfigApplicationContext();
        addEnvironment(context, "cassandra.migration.keyspace-name:test_keyspace");
        addEnvironment(context, "cassandra.migration.with-consensus:true");
        context.register(ClusterConfig.class, CassandraMigrationAutoConfiguration.class);
        context.refresh();
        CqlSession session = context.getBean(CqlSession.class);
        context.getBean(MigrationTask.class);

        List<Row> rows = session.execute("SELECT * FROM schema_migration").all();
        assertThat(rows.size(), is(equalTo(1)));
        Row migration = rows.get(0);
        assertThat(migration.getBoolean("applied_successful"), is(true));
        assertThat(migration.getInstant("executed_at"), is(not(nullValue())));
        assertThat(migration.getString("script_name"), is(CoreMatchers.equalTo("001_create_person_table.cql")));
        assertThat(migration.getString("script"), startsWith("CREATE TABLE"));
    }

    @Test
    public void shouldMigrateDatabaseWhenClusterGivenWithPrefix() {
        AnnotationConfigApplicationContext context =
                new AnnotationConfigApplicationContext();
        addEnvironment(context, "cassandra.migration.keyspace-name:test_keyspace");
        addEnvironment(context, "cassandra.migration.table-prefix:test");
        context.register(ClusterConfig.class, CassandraMigrationAutoConfiguration.class);
        context.refresh();
        CqlSession session = context.getBean(CqlSession.class);
        context.getBean(MigrationTask.class);

        List<Row> rows = session.execute("SELECT * FROM test_schema_migration").all();
        assertThat(rows.size(), is(equalTo(1)));
        Row migration = rows.get(0);
        assertThat(migration.getBoolean("applied_successful"), is(true));
        assertThat(migration.getInstant("executed_at"), is(not(nullValue())));
        assertThat(migration.getString("script_name"), is(CoreMatchers.equalTo("001_create_person_table.cql")));
        assertThat(migration.getString("script"), startsWith("CREATE TABLE"));

    }

    @Configuration
    static class ClusterConfig {
        static final String TEST_KEYSPACE = "test_keyspace";

        private static final String CASSANDRA_INIT_SCRIPT = "cassandraTestInit.cql";
        private static final String LOCALHOST = "127.0.0.1";

        private static final String YML_FILE_LOCATION = "cassandra.yml";
        private CqlSession session;

        @Bean
        public CqlSession session() throws Exception {
            init();
            return session;
        }

        private void init() throws Exception {
            EmbeddedCassandraServerHelper.startEmbeddedCassandra(YML_FILE_LOCATION, 30 * 1000L);
            session = CqlSession.builder()
                    .addContactPoints(Collections.singleton(InetSocketAddress.createUnresolved(LOCALHOST, 9142)))
                    .build();
            loadTestData();
        }

        private void loadTestData() {
            CQLDataSet dataSet = new ClassPathCQLDataSet(CASSANDRA_INIT_SCRIPT, TEST_KEYSPACE);
            CQLDataLoader dataLoader = new CQLDataLoader(session);
            dataLoader.load(dataSet);
        }

    }

}