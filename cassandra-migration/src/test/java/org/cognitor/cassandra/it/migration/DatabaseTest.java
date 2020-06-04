package org.cognitor.cassandra.it.migration;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.Version;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.google.common.collect.Lists;
import org.cognitor.cassandra.CassandraJUnitRule;
import org.cognitor.cassandra.migration.Database;
import org.cognitor.cassandra.migration.MigrationException;
import org.cognitor.cassandra.migration.MigrationRepository;
import org.cognitor.cassandra.migration.MigrationTask;
import org.cognitor.cassandra.migration.keyspace.Keyspace;
import org.cognitor.cassandra.migration.keyspace.NetworkStrategy;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static org.cognitor.cassandra.CassandraJUnitRule.DEFAULT_SCRIPT_LOCATION;
import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * @author Patrick Kranz
 */

@Ignore
public class DatabaseTest {

    @Rule
    public final CassandraJUnitRule cassandra = new CassandraJUnitRule(DEFAULT_SCRIPT_LOCATION, "cassandra.yml");

    @Test
    public void shouldReturnSchemaVersionOfZeroInAnEmptyDatabase() {
        Database database = new Database(cassandra.getSession(), CassandraJUnitRule.TEST_KEYSPACE);
        assertThat(database.getVersion(), is(equalTo(0)));
    }

    @Test
    public void shouldCreateMigrationTableWithPrefixWhenPrefixGiven() {
        new Database(cassandra.getSession(), CassandraJUnitRule.TEST_KEYSPACE, "prefix");
        assertTrue(cassandra.getSession().getMetadata().getKeyspace(CassandraJUnitRule.TEST_KEYSPACE)
                .map(keyspace -> keyspace.getTable("prefix_schema_migration"))
                .isPresent());
        assertTrue(cassandra.getSession().getMetadata().getKeyspace(CassandraJUnitRule.TEST_KEYSPACE)
                .map(keyspace -> keyspace.getTable("schema_migration"))
                .isPresent());
    }

    @Test
    public void shouldApplyMigrationToDatabaseWhenMigrationsAndPrefixAndEmptyDatabaseGiven() {
        final String prefix = "prefix";
        Database database = new Database(cassandra.getSession(), CassandraJUnitRule.TEST_KEYSPACE, prefix);
        MigrationTask migrationTask = new MigrationTask(database, new MigrationRepository("cassandra/migrationtest/successful"));
        migrationTask.migrate();
        // after migration the database object is closed
        database = new Database(cassandra.getSession(), CassandraJUnitRule.TEST_KEYSPACE, prefix);
        assertThat(database.getVersion(), is(equalTo(3)));

        List<Row> results = loadMigrations(prefix);
        assertThat(results.size(), is(equalTo(3)));
        assertThat(results.get(0).getBoolean("applied_successful"), is(true));
        assertThat(results.get(1).getBoolean("applied_successful"), is(true));
        assertThat(results.get(2).getBoolean("applied_successful"), is(true));
    }

    @Test
    public void shouldApplyMigrationToDatabaseWhenMigrationsAndEmptyDatabaseGiven() {
        Database database = new Database(cassandra.getSession(), CassandraJUnitRule.TEST_KEYSPACE);
        MigrationTask migrationTask = new MigrationTask(database, new MigrationRepository("cassandra/migrationtest/successful"));
        migrationTask.migrate();
        // after migration the database object is closed
        database = new Database(cassandra.getSession(), CassandraJUnitRule.TEST_KEYSPACE);
        assertThat(database.getVersion(), is(equalTo(3)));

        List<Row> results = loadMigrations("");
        assertThat(results.size(), is(equalTo(3)));
        assertThat(results.get(0).getBoolean("applied_successful"), is(true));
        assertThat(results.get(0).getInstant("executed_at"), is(not(nullValue())));
        assertThat(results.get(0).getString("script_name"), is(equalTo("001_init.cql")));
        assertThat(results.get(0).getString("script"), is(startsWith("CREATE TABLE")));
        assertThat(results.get(1).getBoolean("applied_successful"), is(true));
        assertThat(results.get(1).getInstant("executed_at"), is(not(nullValue())));
        assertThat(results.get(1).getString("script_name"), is(equalTo("002_add_events_table.cql")));
        assertThat(results.get(1).getString("script"), is(equalTo("CREATE TABLE EVENTS (event_id uuid primary key, event_name varchar);")));
        assertThat(results.get(2).getBoolean("applied_successful"), is(true));
        assertThat(results.get(2).getInstant("executed_at"), is(not(nullValue())));
        assertThat(results.get(2).getString("script_name"), is(equalTo("003_add_another_table.cql")));
        assertThat(results.get(2).getString("script"), is(equalTo("CREATE TABLE THINGS (thing_id uuid primary key, thing_name varchar);")));
    }

    @Test
    public void shouldApplyConcurrentMigrationsToDatabaseWhenMigrationsAndEmptyDatabaseGiven()
            throws InterruptedException, ExecutionException {
        int concurrentTasks = 3;
        ExecutorService executorService = Executors.newFixedThreadPool(concurrentTasks);
        List<Database> databases = Lists.newArrayList();
        List<MigrationTask> migrationTasks = Lists.newArrayList();

        for (int i = 0; i < concurrentTasks; i++) {
            databases.add(new Database(cassandra.getSession(), CassandraJUnitRule.TEST_KEYSPACE));
            migrationTasks.add(
                    new MigrationTask(
                            databases.get(i),
                            new MigrationRepository("cassandra/migrationtest/successful"),
                            true));
        }

        List<Callable<Boolean>> migrations = migrationTasks.stream().map(task -> databaseMigrationTask(task))
                .collect(Collectors.toList());

        // Executing the same migration concurrently with different threads
        List<Future<Boolean>> futures = executorService.invokeAll(migrations);
        for (Future<Boolean> future : futures) {
            future.get();
        }

        Database database = new Database(cassandra.getSession(), CassandraJUnitRule.TEST_KEYSPACE);
        assertThat(database.getVersion(), is(equalTo(3)));

        List<Row> results = loadMigrations("");
        assertThat(results.size(), is(equalTo(3)));
        assertThat(results.get(0).getBoolean("applied_successful"), is(true));
        assertThat(results.get(0).getInstant("executed_at"), is(not(nullValue())));
        assertThat(results.get(0).getString("script_name"), is(equalTo("001_init.cql")));
        assertThat(results.get(0).getString("script"), is(startsWith("CREATE TABLE")));
        assertThat(results.get(1).getBoolean("applied_successful"), is(true));
        assertThat(results.get(1).getInstant("executed_at"), is(not(nullValue())));
        assertThat(results.get(1).getString("script_name"), is(equalTo("002_add_events_table.cql")));
        assertThat(results.get(1).getString("script"),
                is(equalTo("CREATE TABLE EVENTS (event_id uuid primary key, event_name varchar);")));
        assertThat(results.get(2).getBoolean("applied_successful"), is(true));
        assertThat(results.get(2).getInstant("executed_at"), is(not(nullValue())));
        assertThat(results.get(2).getString("script_name"), is(equalTo("003_add_another_table.cql")));
        assertThat(results.get(2).getString("script"),
                is(equalTo("CREATE TABLE THINGS (thing_id uuid primary key, thing_name varchar);")));
    }

    Callable<Boolean> databaseMigrationTask(MigrationTask migrationTask) {
        return () -> {
            migrationTask.migrate();
            return true;
        };
    }

    @Test
    public void shouldNotApplyAnyMigrationWhenDatabaseAndScriptsAreAtSameVersion() {
        Database database = new Database(cassandra.getSession(), CassandraJUnitRule.TEST_KEYSPACE);
        // provide a path without scripts to simulate this
        MigrationRepository repository = new MigrationRepository("migrationtest");
        new MigrationTask(database, repository).migrate();

        assertThat(database.getVersion(), is(equalTo(0)));
    }

    @Test
    public void shouldThrowExceptionAndLogFailedMigrationWhenWrongMigrationScriptGiven() {
        Database database = new Database(cassandra.getSession(), CassandraJUnitRule.TEST_KEYSPACE);
        MigrationRepository repository = new MigrationRepository("cassandra/migrationtest/failing/brokenstatement");
        MigrationException exception = null;
        try {
            new MigrationTask(database, repository).migrate();
        } catch (MigrationException e) {
            exception = e;
        }
        assertThat(exception, is(not(nullValue())));
        assertThat(exception.getMessage(), is(not(nullValue())));
        assertThat(exception.getScriptName(), is(equalTo("001_init.cql")));
        assertThat(exception.getStatement(), is(equalTo("CREATE TABLE PERSON (id uuid primary key, name varcha);")));

        List<Row> results = loadMigrations("");
        assertThat(results.size(), is(equalTo(1)));
        assertThat(results.get(0).getBoolean("applied_successful"), is(false));
        assertThat(results.get(0).getInstant("executed_at"), is(not(nullValue())));
    }

    @Test
    public void shouldCreateKeyspaceWhenDatabaseWithoutKeyspaceAndKeyspaceDefinitionGiven() {
        Database database = new Database(cassandra.getSession(), CassandraJUnitRule.TEST_KEYSPACE);
        assertThat(cassandra.getSession().getMetadata().getKeyspace("new_keyspace"), is(nullValue()));
        Keyspace keyspace = new Keyspace("new_keyspace");
        Database db = new Database(cassandra.getSession(), keyspace);

        KeyspaceMetadata keyspaceMetadata = cassandra.getSession().getMetadata().getKeyspace("new_keyspace").get();
        assertThat(keyspaceMetadata, is(notNullValue()));
        assertThat(keyspaceMetadata.getReplication().get("class"),
                is(equalTo("org.apache.cassandra.locator.SimpleStrategy")));
        assertThat(keyspaceMetadata.getReplication().get("replication_factor"), is(equalTo("1")));
        assertThat(db.getVersion(), is(equalTo(0)));
    }

    @Test
    public void shouldCreateKeyspaceWhenDatabaseWithoutKeyspaceAndNetworkKeyspaceDefinitionGiven() {
        Database database = new Database(cassandra.getSession(), CassandraJUnitRule.TEST_KEYSPACE);
        assertThat(cassandra.getSession().getMetadata().getKeyspace("network_keyspace"), is(nullValue()));
        Keyspace keyspace = new Keyspace("network_keyspace")
                .with(new NetworkStrategy().with("dc1", 1));
        Database db = new Database(cassandra.getSession(), keyspace);

        KeyspaceMetadata keyspaceMetadata = cassandra.getSession().getMetadata().getKeyspace("network_keyspace").get();
        assertThat(keyspaceMetadata, is(notNullValue()));
        assertThat(keyspaceMetadata.getReplication().get("class"),
                is(equalTo("org.apache.cassandra.locator.NetworkTopologyStrategy")));
        assertThat(keyspaceMetadata.getReplication().get("dc1"), is(equalTo("1")));
    }

    @Test
    public void shouldCreateFunctionWhenMigrationScriptWithFunctionGiven() {
        Database database = new Database(cassandra.getSession(), CassandraJUnitRule.TEST_KEYSPACE);
        MigrationTask migrationTask = new MigrationTask(database, new MigrationRepository("cassandra/migrationtest/function"));
        migrationTask.migrate();
        database = new Database(cassandra.getSession(), CassandraJUnitRule.TEST_KEYSPACE);
        assertThat(database.getVersion(), is(equalTo(1)));
        assertThat(cassandra.getSession().getMetadata()
                .getKeyspace(CassandraJUnitRule.TEST_KEYSPACE)
                .get()
                .getFunctions().size(), is(equalTo(1)));
    }

    private List<Row> loadMigrations(String tablePrefix) {
        CqlSession session = cassandra.getSession();
        if (tablePrefix == null || tablePrefix.isEmpty()) {
            return session.execute(
                    SimpleStatement.newInstance("SELECT * FROM schema_migration;")).all();
        }
        return session.execute(
                SimpleStatement.newInstance(String.format("SELECT * FROM %s_schema_migration;", tablePrefix))).all();
    }

    @Test
    public void testCassandraVersionCheck() {
        Database database = new Database(cassandra.getSession(), CassandraJUnitRule.TEST_KEYSPACE);
        assertThat(database.isVersionAtLeastV2(Version.parse("1.1.14")), is(false));
        assertThat(database.isVersionAtLeastV2(Version.parse("1.2.19")), is(false));
        assertThat(database.isVersionAtLeastV2(Version.parse("2.0.10")), is(true));
        assertThat(database.isVersionAtLeastV2(Version.parse("2.1.19")), is(true));
        assertThat(database.isVersionAtLeastV2(Version.parse("2.2.14")), is(true));
        assertThat(database.isVersionAtLeastV2(Version.parse("3.0.15")), is(true));
        assertThat(database.isVersionAtLeastV2(Version.parse("3.11.4")), is(true));
        assertThat(database.isVersionAtLeastV2(Version.parse("4.0")), is(true));
        database.close();

    }

    @Test
    public void supportsMultilineStatements() {
        Database database = new Database(cassandra.getSession(), CassandraJUnitRule.TEST_KEYSPACE);
        MigrationRepository repository = new MigrationRepository("cassandra/migrationtest/multiline");
        MigrationTask migrationTask = new MigrationTask(database, repository);
        migrationTask.migrate();
    }

}
