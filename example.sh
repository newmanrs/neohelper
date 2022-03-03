neohelper query \
"unwind  \$jsons as json
MERGE (p:Person {
    name : json.name,
    age : json.age
    })
return count(p) as nodes_merged" \
-j '{"name" : "John Jackson", "age" : "45" }' \
-j '{"name" : "Jack Johnson", "age" : "53" }' \
--write \
--verbose
