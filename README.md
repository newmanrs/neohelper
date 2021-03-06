# neohelper
Some command line tooling I find helpful when creating/debugging scripts loading Neo4j databases.

# Installation

Clone repository, `pip3 install neohelper`.

# Usage

The CLI for this project is built using [click](https://click.palletsprojects.com/).  
You can get the list of current commands by just invoking the script `neohelper` from the command line.

```
Usage: neohelper [OPTIONS] COMMAND [ARGS]...

  Interface for monitoring and interacting with Neo4j databases. Invoke
  `neohelper command --help` for details on each command.

Options:
  --uri TEXT            Database uri  [default: neo4j://localhost:7687]
  --db_pw_env_var TEXT  Environmental var containing neo4j database password
                        [default: NEO4J_PW]
  --help                Show this message and exit.

Commands:
  count          Display count of nodes and edges database
  count-labels   Count of each node label
  detach-delete  Delete all nodes and relationships in the database
  query          Perform given cypher query, with optional parameters
```

Each subcommand to the script has its own manual page such as `neohelper query --help`.

# Examples

## Json inputs

```
neohelper query \
"with \$params as jsons
unwind  jsons as json
MERGE (p:Person {
        name : json.name,
            age : json.age
                })
return count(p) as nodes_merged" \
-j '{"name" : "John Jackson", "age" : "45" }' \
-j '{"name" : "Jack Johnson", "age" : "53" }' \
--mode 'write' \
--verbose
```

should give outputs as:

```
Input query is:
with $params as jsons
unwind  jsons as json
MERGE (p:Person {
    name : json.name,
    age : json.age
    })
return count(p) as nodes_merged

Parsing json parameters:
{"name" : "John Jackson", "age" : "45" }
{"name" : "Jack Johnson", "age" : "53" }

Results:
{'nodes_merged': 2}
```

## Crude database load monitoring

Use watch command to loop running node counts

`watch -n 1 neohelper count-labels`
