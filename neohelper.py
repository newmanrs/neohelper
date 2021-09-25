from neo4j import GraphDatabase
import os
import json
import click

@click.group()
@click.option('--uri',
    default = "neo4j://localhost:7687",
    help = "Database uri",
    show_default = True)
@click.option('--db_pw_env_var',
    default = 'NEO4J_PW',
    help = "Environmental var containing neo4j database password",
    show_default = True)
@click.pass_context
def cli(ctx,uri,db_pw_env_var):
    """ Interface for monitoring and interacting with Neo4j databases """

    try:
        pw = os.environ[db_pw_env_var]
    except KeyError as e:
        msg = "No environment variable `NEO4J_PW` found.  Consider running export NEO4J_PW='yourpassword' in the current shell environment or in your shell config file."
        raise KeyError(msg)

    driver = GraphDatabase.driver(uri, auth=("neo4j", pw))

    ctx.obj = {
        'driver':driver,
        }


@cli.command()
@click.pass_context
def count(ctx):
    """ Display count nodes in database """
    driver = ctx.obj['driver']

    #Call Driver, _count, and read transaction...
    with driver.session() as session:
        srt = driver.session().read_transaction
        c = srt(_count)

def _count(tx):
    query = """
        OPTIONAL MATCH (n) with count(n) as node_count
        OPTIONAL MATCH ()-[r]->()
        with node_count, count(r) as edge_count
        RETURN {node_count : node_count, edge_count : edge_count} as counts
    """
    records = tx.run(query)
    counts = records.single()['counts']
    nc = counts['node_count']
    ec = counts['edge_count']
    click.echo(f"Database contains {nc} nodes and {ec} relationships.")
    return counts

@cli.command()
@click.pass_context
@click.option('--labels', '-l', type=str,
    help="Specify list of node labels to count as quoted string 'Label1 Label2'.  Returns -1 if no node by that label exists")
def count_labels(ctx, *args, **kwargs):
    """ Count of each node label """

    driver = ctx.obj['driver']
    with driver.session() as session:
        srt = driver.session().read_transaction
        label_counts = srt(_count_labels,*args, **kwargs)

def _count_labels(tx, *args, **kwargs):
    query = """
    call db.labels() yield label
    match (n) where label in labels(n)
    with label, count(n) as count order by count DESC
    return collect({label : label, count : count}) as label_counts
    """
    records = tx.run(query)
    results = records.single()['label_counts']
    labels = kwargs['labels']
    if labels is not None:
        labels = labels.split()
        lcl = []
        for label in labels:
            lc = {'label' : label, 'count' : -1}
            for r in results:
                if label == r['label']:
                    lc['count'] = r['count']
            lcl.append(lc)

        results = sorted(lcl, key = lambda k : k['count'],reverse=True)

    if len(results) == 0:
        click.echo("Database has no nodes")
    else:
        #click.echo("Database contains :")
        label_len = max([len(lc['label']) for lc in results])
        for lc in results:
            l = lc['label'].ljust(label_len)
            c = lc['count']
            click.echo(f"{l} : {c}")

    return results

@cli.command()
@click.pass_context
@click.argument('query',type=str)
@click.option('--mode',
    type=click.Choice(['read', 'write'],
    case_sensitive=False),
    default = 'read',
    show_default = True)
@click.option('--json','-j',
    multiple=True)
def query(ctx, *args, **kwargs):
    """ Perform given cypher query """
    l = []
    for param in kwargs['json']:
        l.append(json.loads(param))
    query = kwargs['query']
    mode = kwargs['mode']

    results = _query(ctx, query, l, mode)
    for row in results:
        click.echo(row)

def _query(ctx, query, params, mode):
    with ctx.obj['driver'].session() as session:
        if mode == 'read':
            txfn = session.read_transaction
        else:
            txfn = session.write_transaction
    return txfn(_tx_func, query, params)

def _tx_func(tx, query, params):
    if params:
        print(query)
        print(params)
        results = tx.run(query,params=params)
    else:
        results = tx.run(query)
    l = []
    for r in results:
        click.echo(r)
        keys = r.keys()
        values = r.values()
        d = dict()
        for k,v in zip(keys,values):
            d[k]=v
        l.append(d)

    return l

@cli.command()
@click.pass_context
def detach_delete(ctx):
    """  Delete all nodes and relationships in the database """
    driver = ctx.obj['driver']
    with driver.session() as session:
        swt = driver.session().write_transaction
        swt(_detach_delete)
        swt(_count)

def _detach_delete(tx):
    query = """
    MATCH (n) DETACH DELETE (n)
    """
    tx.run(query)
