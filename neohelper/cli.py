import json
import click
import neohelper


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
def cli(user, pw, uri):
    """
    Interface for monitoring and interacting with Neo4j databases.
    Invoke `neohelper command --help` for details on each command.
    """

    neohelper.init_neo4j_driver(user, pw, uri)


@cli.command()
def count():
    """ Display count of nodes and edges database """

    query = """
        OPTIONAL MATCH (n) with count(n) as node_count
        OPTIONAL MATCH ()-[r]->()
        with node_count, count(r) as edge_count
        RETURN {node_count : node_count, edge_count : edge_count} as counts
    """

    result = neohelper._read_query(query)
    nc = result['counts']['node_count']
    ec = result['counts']['edge_count']
    click.echo(f"Database contains {nc} nodes and {ec} relationships")


@cli.command()
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
def count_node_labels(*args, **kwargs):
    """ Count of nodes by label """

    query = """
    call db.labels() yield label
    match (n) where label in labels(n)
    with label, count(n) as count order by count DESC
    return collect({label : label, count : count}) as label_counts
    """
    record = neohelper._read_query(query)
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
        label_len = max([len(lc['label']) for lc in results])
        for lc in results:
            l = lc['label'].ljust(label_len)
            c = lc['count']
            click.echo(f"{l}: {c}")


@cli.command()
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
def count_relationship_types(*args, **kwargs):
    """ Count of relationships by type """

    query = """
    CALL db.relationshipTypes() YIELD relationshipType as type
    return collect(type) as relationship_types
    """
    record = neohelper._read_query(query)

    results = []
    # Can't parameterize over type in native cypher (need APOC) or
    # to query the relations type by type as done here.
    for t in record['relationship_types']:
        query = f"""
        match ()-[t:{t}]->()
        return count(t) as count
        """
        res = neohelper._read_query(query)
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
def query(*args, **kwargs):
    """
    Perform given cypher query, with optional parameters

    Example:

    \b
    neohelper query \\
    "unwind  \\$jsons as json
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

    dlist = []

    if verbose:
        click.echo("Input query is:\n{}\n".format(query))

    if jsons:
        if verbose:
            click.echo("Parsing json parameters:")
    for j in jsons:
        if verbose:
            click.echo(j)
        dlist.append(json.loads(j))

    if jsons:
        if '$json' not in query:
            msg = "Received query:\n{}\n".format(query) + \
                " Query with parameters must contain '$json'. " \
                " Did you forget to escape with backslash?"
            raise AttributeError(msg)

    if mode == 'write':
        results = neohelper._write_query(query, jsons=dlist)
    else:
        results = neohelper._read_query(query, jsons=dlist)

    if verbose:
        click.echo("Results:")

    if isinstance(results, list):
        for row in results:
            click.echo(row)
    else:
        click.echo(f"{results}")


@cli.command()
def detach_delete():
    """ Delete all nodes and relationships """

    query = """
    MATCH (n) DETACH DELETE (n)
    """
    neohelper._write_query(query, mode='write')


@cli.command()
@click.option(
    '--indent', '-i',
    type=int,
    default=None,
    help=(
        "Set indentation of json printout"
        )
    )
def show_indexes(*args, **kwargs):
    """
    Print database indexes
    """
    results = neohelper.get_all_indexes()
    for r in results:
        click.echo(json.dumps(r, indent=kwargs['indent']))
