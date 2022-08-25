import os
import warnings
import atexit
from neo4j import GraphDatabase
from neo4j import ExperimentalWarning

warnings.filterwarnings("ignore", category=ExperimentalWarning)


class NeoHelper():

    def __init__(self):
        self.driver = None    # Store Neo4j driver once initialized
        self.database = None  # Name of database to use
        self.verbose = False
        self.print_command = print

    def set_verbose(self, verbose: bool):
        self.verbose = verbose

    def set_database(self, database: str):
        self.database = database

    @staticmethod
    def _get_env_variable(var: str):
        try:
            var = os.environ[var]
        except KeyError as e:
            msg = f"Environmental variable '{var}' not found"
            raise KeyError(msg) from e
        return var

    def init_neo4j_driver(
            self,
            user_ev='NEO4J_USER',
            pw_ev='NEO4J_PW',
            uri_ev='NEO4J_URI'
            ):
        """
        Create new driver if none exists yet, otherwise
        return existing driver.
        Retrieves username, password, and URI from env vars
        """

        if self.driver is None:

            errs = []
            try:
                user = self._get_env_variable(user_ev)
            except KeyError as e:
                errs.append(e)
            try:
                pw = self._get_env_variable(pw_ev)
            except KeyError as e:
                errs.append(e)
            try:
                uri = self._get_env_variable(uri_ev)
            except KeyError as e:
                errs.append(e)
            if errs:
                raise KeyError(errs)

            driver = GraphDatabase.driver(uri, auth=(user, pw))
            driver.verify_connectivity()
            self.driver = driver

            atexit.register(self.close_driver)

        if self.database is None:
            self.print_command(
                "No database selected, querying for default")
            self.database = 'system'
            query = "show default database yield name"
            self.set_database(self.read_query(query)['name'])

    def get_driver(self):
        if self.driver is None:
            msg = "Neo4j driver not initialized"
            raise RuntimeError(msg)
        return self.driver

    def read_query(self, query, database=None, **kwargs):

        if database is None:
            database = self.database

        if self.verbose:
            self.print_command(
                "Executing read query to database= "
                f"'{database}'"
                )
            self.print_command(f"{query}")


        with self.get_driver().session(database=database) as session:
            s = session.read_transaction
            return s(self._tx_func, query, **kwargs)

    def write_query(self, query, database=None, **kwargs):

        if database is None:
            database = self.database

        if self.verbose:
            self.print_command(f"Executing write query to database='{database}'")
            self.print_command(f"{query}")

        with self.get_driver().session(database=database) as session:
            s = session.write_transaction
            return s(self._tx_func, query, **kwargs)

    def _tx_func(self, tx, query, **kwargs):

        results = tx.run(query, **kwargs)
        l = []
        # Convert the db response objects into
        # a regular list of dictionaries
        for r in results:
            keys = r.keys()
            values = r.values()
            d = dict()
            for k, v in zip(keys, values):
                d[k] = v
            l.append(d)
        if len(l) == 0:  # No results
            return None
        elif len(l) == 1:  # One result, unpack list
            return l[0]
        return l

    def get_all_indexes(self):
        query = "SHOW INDEXES"
        return self.read_query(query)

    def get_database_names(self):
        query = """
            SHOW DATABASES yield name
            return collect(name) as names
        """
        return self.read_query(query, database='system')['names']

    def create_database(self, database):
        query = f"CREATE DATABASE {database}"
        self.write_query(query, database='system')

    def drop_database(self, database):
        query = f"DROP DATABASE {database}"
        self.write_query(query, database='system')

    def clear_database(self, database):

        db_names = self.get_database_names()
        if database in db_names:
            query = f"CREATE OR REPLACE DATABASE {database}"
            self.write_query(query, database='system')
        else:
            raise ValueError(f"Database {database} not found")

    def apoc_version(self):
        query = "RETURN apoc.version() as v"
        return self.read_query(query)['v']


    def version(self):
        query = "call dbms.components()"
        return self.read_query(query)


    def close_driver(self):
        self.driver.close()

# Instance of class for CLI to use.  Hide in
# module to avoid passing it to every command
# using dozens of @click.pass_context decorators
nh = NeoHelper()
