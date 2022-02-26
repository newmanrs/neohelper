import os
import warnings
from neo4j import GraphDatabase
from neo4j import ExperimentalWarning
import neohelper

warnings.filterwarnings("ignore", category=ExperimentalWarning)

# Store copy of driver in module once initialized
driver = None


def _get_env_variable(var):
    try:
        var = os.environ[var]
    except KeyError as e:
        msg = f"Environmental variable '{var}' not found"
        raise KeyError(msg) from e
    return var


def init_neo4j_driver(user_ev, pw_ev, uri_ev):

    if neohelper.utils.driver is None:

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
        neohelper.utils.driver = driver

    return neohelper.utils.driver
