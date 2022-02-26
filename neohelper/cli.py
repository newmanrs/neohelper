import json
import click
import neohelper.utils


@click.group()
@click.option(
    '--user',
    default='NEO4J_USER',
    help="Environmental variable containing neo4j username"
    )
@click.option(
    '--pw',
    default='NEO4J_PW',
    help="Environmental variable containing neo4j database password",
    show_default=True
    )
@click.option(
    '--uri',
    default="NEO4J_URI",
    help="Environmental variable storing Neo4j"
         "uri (i.e. neo4j://localhost:7687)",
    show_default=True
    )
@click.pass_context
def cli(ctx, user, pw, uri):
    """
    Interface for monitoring and interacting with Neo4j databases.
    Invoke `neohelper command --help` for details on each command.
    """
    driver = neohelper.utils.init_neo4j_driver(user, pw, uri)
    ctx.obj = {'driver': driver}  # Store in click pass_context


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
@click.option(
    '--labels', '-l',
    type=str,
    multiple=True,
    help=(
        "Specify node labels to be counted. "
        "Option can be used multiple times "
        " to specify multiple labels. "
        "Returns -1 if no node by that label exists"
        )
    )
def count_node_labels(ctx, *args, **kwargs):
    """ Count of nodes by label """

    query = """
    call db.labels() yield label
    match (n) where label in labels(n)
    with label, count(n) as count order by count DESC
    return collect({label : label, count : count}) as label_counts
    """
    record = _query(ctx, query)
    labels = kwargs['labels']
    results = record['label_counts']

    # Filter from the results for all labels for ones given.
    if labels:
        lcl = []
        for label in labels:
            lc = {'label': label, 'count': -1}
            for r in results:
                if label == r['label']:
                    lc['count'] = r['count']
            lcl.append(lc)

        results = sorted(lcl, key=lambda k: k['count'], reverse=True)

    if len(results) == 0:
        click.echo("Database has no nodes")
    else:
        # click.echo("Database contains :")
        label_len = max([len(lc['label']) for lc in results])
        for lc in results:
            l = lc['label'].ljust(label_len)
            c = lc['count']
            click.echo(f"{l} : {c}")


@cli.command()
@click.pass_context
@click.option(
    '--labels', '-l',
    type=str,
    multiple=True,
    help=(
        "Specify relationship types to be counted. "
        "Option can be used multiple times "
        "to specify multiple labels. "
        "Returns -1 if no node by that label exists"
        )
    )
def count_relationship_types(ctx, *args, **kwargs):
    """ Count of relationships by type """

    query = """
    CALL db.relationshipTypes() YIELD relationshipType as type
    return collect(type) as relationship_types
    """
    record = _query(ctx, query)

    results = []
    # Can't parameterize over type in native cypher (need APOC) or
    # to query the relations type by type as done here.
    for t in record['relationship_types']:
        query = f"""
        match ()-[t:{t}]->()
        return count(t) as count
        """
        res = _query(ctx, query)
        results.append({'label': t, 'count': res['count']})

    results = sorted(results, key=lambda k: k['count'], reverse=True)

    labels = kwargs['labels']

    # Filter from the results for all labels for ones given.
    if labels:
        lcl = []
        for label in labels:
            lc = {'label': label, 'count': -1}
            for r in results:
                if label == r['label']:
                    lc['count'] = r['count']
            lcl.append(lc)

        results = sorted(lcl, key=lambda k: k['count'], reverse=True)

    if len(results) == 0:
        click.echo("Database has no relationships")
    else:
        label_len = max([len(lc['label']) for lc in results])
        for lc in results:
            l = lc['label'].ljust(label_len)
            c = lc['count']
            click.echo(f"{l} : {c}")


@cli.command()
@click.pass_context
@click.argument('query', type=str)
@click.option(
    '--mode',
    type=click.Choice(
        ['read', 'write'],
        case_sensitive=False),
    default='read',
    show_default=True)
@click.option(
    '--json', '-j',
    multiple=True,
    help=(
        "Add json string to the query as a list variable $params"
        " containing each as a dictionary"
        "Use multiple -j flags, one per each to fill list.  Be sure "
        "in your query to escape \\$params to prevent your shell "
        "from doing variable substitution"
        )
    )
@click.option(
    '--verbose', '-v',
    default=False,
    is_flag=True)
def query(ctx, *args, **kwargs):
    """
    Perform given cypher query, with optional parameters

    Example:

    \b
    neohelper query \\
    "with \\$params as jsons
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

    if l:
        if '$param' not in query:
            msg = "Received query:\n{}\n".format(query) + \
                " Query with parameters must contain '$param'. " \
                " Did you forget to escape with backslash?"
            raise AttributeError(msg)

    results = _query(ctx, query, l, mode)
    if verbose:
        click.echo(f"\nResults:\n{results}")

    if isinstance(results, list):
        for row in results:
            click.echo(row)
    else:
        click.echo(f"{results}")


def _query(ctx, query, params=None, mode='read'):

    with ctx.obj['driver'].session() as session:
        if mode == 'read':
            txfn = session.read_transaction
        else:
            txfn = session.write_transaction
        return txfn(_tx_func, query, params)


def _tx_func(tx, query, params):
    if params:
        results = tx.run(query, params=params)
    else:
        results = tx.run(query)
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


@cli.command()
@click.pass_context
def detach_delete(ctx):
    """ Delete all nodes and relationships """

    query = """
    MATCH (n) DETACH DELETE (n)
    """
    _query(ctx, query, mode='write')