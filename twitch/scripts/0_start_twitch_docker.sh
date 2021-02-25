docker run   --volume=$(pwd)/twitch/neo4j/data:/data   --volume=$(pwd)/twitch/neo4j/logs:/logs   --volume=$(pwd)/twitch/neo4j/plugins:/plugins   --env NEO4J_AUTH=none   --env NEO4J_dbms_security_procedures_unrestricted=gds.*,apoc.*   --env NEO4J_dbms_security_procedures_allowlist=gds.*,apoc.*   --name twitch_neo4j_docker   -p 8474:7474 -p 8687:7687   -d  neo4j:4.2.1

docker run --volume=$(pwd)/twitch/mongo/data:/data/db  -p 27817:27017 -p 27818:27018 -p 27819:27019 --name=twitch_mongo_docker -d mongo
