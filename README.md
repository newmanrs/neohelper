# neohelper
Some command line tooling I find helpful when creating/debugging scripts loading Neo4j databases.

# Installation

Clone repository, `pip3 install neohelper`.

# Usage

The CLI for this project is built using [click](https://click.palletsprojects.com/).  
You can get the list of current commands by just invoking the script `neohelper` from the command line.

```
Usage: neohelper [OPTIONS] COMMAND [ARGS]...

  Interface for monitoring and interacting with Neo4j databases

Options:
  --uri TEXT            Database uri  [default: neo4j://localhost:7687]
  --db_pw_env_var TEXT  Environmental var containing neo4j database password
                        [default: NEO4J_PW]
  --help                Show this message and exit.

Commands:
  count          Display count nodes in database
  count-labels   Count of each node label
  detach-delete  Delete all nodes and relationships in the database
```

Each subcommand to the script has its own manual page such as `neohelper count-labels --help` describing additional options or flags if any.  

At the time of writing this readme, a useful command to run during loading neo4j databases is `watch -n 1 neohelper count-labels`, which shows the current count of nodes of each label type in the database and updates your terminal window every 1s.  And of course `neohelper detach-delete` for a quick wipe.
