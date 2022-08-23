import os
import warnings
import atexit
from neo4j import GraphDatabase
from neo4j import ExperimentalWarning
import neohelper

warnings.filterwarnings("ignore", category=ExperimentalWarning)


driver = None    # Store Neo4j driver once initialized
database = None  # Name of database to use


def set_database(database: str):
    neohelper.database = database


def _get_env_variable(var: str):
    try:
        var = os.environ[var]
    except KeyError as e:
        msg = f"Environmental variable '{var}' not found"
        raise KeyError(msg) from e
    return var


def init_neo4j_driver(
        user_ev='NEO4J_USER',
        pw_ev='NEO4J_PW',
        uri_ev='NEO4J_URI'):
    """
    Create new driver if none exists yet, otherwise
    return existing driver.
    Retrieves username, password, and URI from env vars
    """

    if neohelper.driver is None:

        errs = []
        try:
            user = _get_env_variable(user_ev)
        except KeyError as e:
            errs.append(e)
        try:
            pw = _get_env_variable(pw_ev)
        except KeyError as e:
            errs.append(e)
        try:
            uri = _get_env_variable(uri_ev)
        except KeyError as e:
            errs.append(e)
        if errs:
            raise KeyError(errs)

        driver = GraphDatabase.driver(uri, auth=(user, pw))
        driver.verify_connectivity()
        neohelper.driver = driver

        atexit.register(close_driver)

    # if neohelper.database is None:
    #    query = "show default database yield name"
    #    set_database(read_query(query)['name'])


def get_driver():
    if neohelper.driver is None:
        msg = "Neo4j driver not initialized"
        raise RuntimeError(msg)
    return neohelper.driver


def read_query(query, **kwargs):

    with get_driver().session(database=neohelper.database) as session:
        s = session.read_transaction
        return s(_tx_func, query, **kwargs)


def write_query(query, **kwargs):

    with get_driver().session(database=neohelper.database) as session:
        s = session.write_transaction
        return s(_tx_func, query, **kwargs)


def _tx_func(tx, query, **kwargs):

    results = tx.run(query, **kwargs)
    l = []
    for r in results:
        keys = r.keys()
        values = r.values()
        d = dict()
        for k, v in zip(keys, values):
            d[k] = v
        l.append(d)
    if len(l) == 0:
        return None
    elif len(l) == 1:
        return l[0]
    return l


def get_all_indexes():
    query = "SHOW INDEX"
    results = write_query(query)
    return results


def get_database_names():
    query = """
        SHOW DATABASES yield name
        return collect(name) as names
    """
    return read_query(query)['names']


def create_database(database):
    query = f"CREATE DATABASE {database}"
    write_query(query)


def drop_database(database):
    query = f"DROP DATABASE {database}"
    write_query(query)


def clear_database(database):

    db_names = get_database_names()
    if database in db_names:
        query = f"CREATE OR REPLACE DATABASE {database}"
        write_query(query)
    else:
        raise ValueError(f"Database {database} not found")


def close_driver():
    neohelper.driver.close()
