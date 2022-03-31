
mainClass=org.apache.spark.disc.SubgraphCounting
JAR="./DISC-assembly-0.1.jar"

java --class $mainClass $JAR "$@"