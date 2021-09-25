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
    """
    Interface for monitoring and interacting with Neo4j databases.
    Invoke `neohelper command --help` for details on each command.
    """

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
    """ Display count of nodes and edges database """

    query = """
        OPTIONAL MATCH (n) with count(n) as node_count
        OPTIONAL MATCH ()-[r]->()
        with node_count, count(r) as edge_count
        RETURN {node_count : node_count, edge_count : edge_count} as counts
    """

    result = _query(ctx, query)
    nc = result['counts']['node_count']
    ec = result['counts']['edge_count']
    click.echo(f"Database contains {nc} nodes and {ec} relationships")



@cli.command()
@click.pass_context
@click.option('--labels', '-l', type=str,
    help="Specify list of node labels to count as quoted string 'Label1 Label2'.  Returns -1 if no node by that label exists")
def count_labels(ctx, *args, **kwargs):
    """ Count of each node label """

    query = """
    call db.labels() yield label
    match (n) where label in labels(n)
    with label, count(n) as count order by count DESC
    return collect({label : label, count : count}) as label_counts
    """
    record = _query(ctx,query)
    labels = kwargs['labels']
    results = record['label_counts']

    #Filter from the results for all labels for ones given.
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

@cli.command()
@click.pass_context
@click.argument('query',type=str)
@click.option('--mode',
    type=click.Choice(['read', 'write'],
    case_sensitive=False),
    default = 'read',
    show_default = True)
@click.option('--json','-j',
    multiple=True,
    help = "Add json string to the query as a list variable $params.  Use multiple -j flags, one per each to fill list.  Be sure in your query to escape \"UNWIND \$params\"")
@click.option('--verbose', '-v',
    default = False,
    is_flag = True)
def query(ctx, *args, **kwargs):
    """ 
    Perform given cypher query, with optional parameters

    Example:

    \b
    neohelper query \\
    "with \$params as jsons
    unwind  jsons as json
    MERGE (p:Person {
        name : json.name,
        age : json.age
        })
    return count(p) as nodes_merged" \\
    -j '{"name" : "John Jackson", "age" : "45" }' \\
    -j '{"name" : "Jack Johnson", "age" : "53" }' \\
    --mode 'write' \\
    --verbose


    """

    query = kwargs['query']
    mode = kwargs['mode']
    jsons = kwargs['json']
    verbose = kwargs['verbose']

    l = []

    if verbose:
        click.echo("Input query is:\n{}\n".format(query))

    if jsons:
        if verbose:
            click.echo("Parsing json parameters:")
    for j in jsons:
        if verbose:
            click.echo(j)
        l.append(json.loads(j))

    results = _query(ctx, query, l, mode)
    if verbose:
        click.echo("\nResults:")

    if isinstance(results,list):
        for row in results:
            click.echo(row)
    else:
        click.echo(results)

def _query(ctx, query, params =[], mode = 'read'):
    with ctx.obj['driver'].session() as session:
        if mode == 'read':
            txfn = session.read_transaction
        else:
            txfn = session.write_transaction
    return txfn(_tx_func, query, params)

def _tx_func(tx, query, params):
    if params:
        results = tx.run(query,params=params)
    else:
        results = tx.run(query)
    l = []
    for r in results:
        #click.echo(r)
        keys = r.keys()
        values = r.values()
        d = dict()
        for k,v in zip(keys,values):
            d[k]=v
        l.append(d)
    if len(l) == 0:
        return None
    elif len(l) == 1:
        return l[0]
    return l

@cli.command()
@click.pass_context
def detach_delete(ctx):
    """  Delete all nodes and relationships in the database """

    query = """
    MATCH (n) DETACH DELETE (n)
    """
    _query(ctx, query, mode='write')
