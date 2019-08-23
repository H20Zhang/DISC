#!/usr/bin/env bash

prefix="/user/hzhang/subgraph/Dataset"
JAR="./ADJ-assembly-1.2.6.jar"
ks=(32)


Test_HCUBEGJ() {
JAR="./ADJ-assembly-0.1.1.jar"
mainClass=hzhang.framework.test.exp.entry.HyperCubeExp
#input=(as lj webB wikiT  enwiki-2013 orkut)
 input=(lj)
for i in ${input[@]}; do
		data=$i
		file="${prefix}/${data}_undir"


		#3 p
		# querys=(triangle house threeTriangle near5Clique)
		# querys=(threeTriangle)
		# querys=(threeCenterTriangle twoHeadTriangle)


#		test communication only time
#		querys=(house1)
#		# querys=(lazyNear5Clique)
#		for query in ${querys[@]}; do
#				echo "----------------------------------"
#				echo executing $i $query $k
#
#				SECONDS=0
#				runSpark-logo.sh  --num-executors 32  --class $mainClass $JAR $file $query 11 true
#
#				duration=$SECONDS
#				echo "executing $i $duration seconds elapsed."
#		done

#		test communication only under optimal share
		querys=(house3)
		for query in ${querys[@]}; do
				echo "----------------------------------"
				echo executing $i $query $k

				SECONDS=0
				runSpark-logo.sh  --num-executors 32  --class $mainClass $JAR $file $query 11 true

				duration=$SECONDS
				echo "executing $i $duration seconds elapsed."
		done

#		test total cost under optimal share
		querys=(house3)
		for query in ${querys[@]}; do
				echo "----------------------------------"
				echo executing $i $query $k

				SECONDS=0
				runSpark-logo.sh  --num-executors 32  --class $mainClass $JAR $file $query 11 false

				duration=$SECONDS
				echo "executing $i $duration seconds elapsed."
		done



		querys=()

		#4 p
		# querys=(square fourClique)
		for query in ${querys[@]}; do
				echo "----------------------------------"
				echo executing $i $query $k

				SECONDS=0
				runSpark-logo.sh  --num-executors 32  --class $mainClass $JAR $file $query 6 false

				duration=$SECONDS
				echo "executing $i $duration seconds elapsed."
		done
		querys=()

		#2 p
		# querys=(chordalSquare)
		for query in ${querys[@]}; do
				echo "----------------------------------"
				echo executing $i $query $k

				SECONDS=0
				runSpark-logo.sh  --num-executors 32  --class $mainClass $JAR $file $query 36 false

				duration=$SECONDS
				echo "executing $i $duration seconds elapsed."
		done
done
}

Test_Speed(){
mainClass=org.apache.spark.Logo.Experiment.LegoMainEntry
input=(as lj webB wikiT  enwiki-2013 orkut)
# input=(as)
for i in ${input[@]}; do
		data=$i
		file="${prefix}/${data}_undir"

		#3 p
		# querys=(triangle house threeTriangle near5Clique)
		# querys=(threeTriangle)
		# querys=(threeCenterTriangle twoHeadTriangle)
		querys=(twoHeadTriangle fourCliqueEdge)
		# querys=(lazyNear5Clique)
		for query in ${querys[@]}; do
				echo "----------------------------------"
				echo executing $i $query $k

				SECONDS=0
				runSpark-logo.sh  --num-executors 32  --class $mainClass $JAR $file $query 11 false

				duration=$SECONDS
				echo "executing $i $duration seconds elapsed."
		done
		querys=()

		#4 p
		# querys=(square fourClique)
		for query in ${querys[@]}; do
				echo "----------------------------------"
				echo executing $i $query $k

				SECONDS=0
				runSpark-logo.sh  --num-executors 32  --class $mainClass $JAR $file $query 6 false

				duration=$SECONDS
				echo "executing $i $duration seconds elapsed."
		done
		querys=()

		#2 p
		# querys=(chordalSquare)
		for query in ${querys[@]}; do
				echo "----------------------------------"
				echo executing $i $query $k

				SECONDS=0
				runSpark-logo.sh  --num-executors 32  --class $mainClass $JAR $file $query 36 false

				duration=$SECONDS
				echo "executing $i $duration seconds elapsed."
		done
done
}

Test_PatternStatistic(){
mainClass=hzhang.framework.test.exp.entry.StatisticExp
input=(orkut)

for i in ${input[@]}; do
		data=$i
		file="${prefix}/${data}_undir"

		#3 p
#		querys=(house near5Clique)
		querys=(near5Clique)
		for query in ${querys[@]}; do
				echo "----------------------------------"
				echo executing Test_PatternStatistic $i $query

				SECONDS=0
				runSpark-logo.sh  --num-executors 32  --class $mainClass $JAR $file $query 11

				duration=$SECONDS
				echo "executing $i $duration seconds elapsed."
		done
done
}



TestForLoop(){

# querys=(triangle square chordalSquare house threeTriangle near5Clique)
JAR="./Logo-assembly-1.2.6.jar"

for i in ${input[@]}; do
	data=$i
	file="${prefix}/${data}_undir"

	#3 p
	# querys=(triangle house threeTriangle near5Clique)
	querys=(threeTriangle)
	for query in ${querys[@]}; do
			echo "----------------------------------"
			echo executing $i $query

			SECONDS=0
			runSpark-logo.sh  --num-executors 32  --class $mainClass $JAR $file $query 11 true

			duration=$SECONDS
			echo "executing $i $duration seconds elapsed."
	done
	querys=()

	#4 p
	# querys=(square fourClique)
	for query in ${querys[@]}; do
			echo "----------------------------------"
			echo executing $i $query $k

			SECONDS=0
			runSpark-logo.sh  --num-executors 32  --class $mainClass $JAR $file $query 6 true

			duration=$SECONDS
			echo "executing $i $duration seconds elapsed."
	done
	querys=()

	#2 p
	# querys=(chordalSquare)
	for query in ${querys[@]}; do
			echo "----------------------------------"
			echo executing $i $query $k

			SECONDS=0
			runSpark-logo.sh  --num-executors 32  --class $mainClass $JAR $file $query 36 true

			duration=$SECONDS
			echo "executing $i $duration seconds elapsed."
	done
done
}

TestEager(){
mainClass=org.apache.spark.Logo.Experiment.LegoMainEntry

input=(as wikiT enwiki-2013 webB)
# input=(orkut)
querys=(eagerNear5Clique)
for i in ${input[@]}; do
		data=$i
		file="${prefix}/${data}_undir"

		#3 p
		for query in ${querys[@]}; do
				echo "----------------------------------"
				echo executing $i $query $k

				SECONDS=0
				runSpark-logo.sh  --num-executors 32  --class $mainClass $JAR $file $query 15 false

				duration=$SECONDS
				echo "executing $i $duration seconds elapsed."
		done
done
querys=()

input=(lj)

# querys=(eagerThreeTriangle eagerHouse)

for i in ${input[@]}; do
		data=$i
		file="${prefix}/${data}_undir"

		#3 p
		for query in ${querys[@]}; do
				echo "----------------------------------"
				echo executing $i $query $k

				SECONDS=0
				runSpark-logo.sh  --num-executors 32  --class $mainClass $JAR $file $query 30 false

				duration=$SECONDS
				echo "executing $i $duration seconds elapsed."
		done
done
}




TestGJ(){
mainClass=org.apache.spark.Logo.Experiment.LegoMainEntryHyberCubeGJ
JAR="./HyperCube-assembly-1.2.6.jar"

# input=(enwiki-2013 orkut)
# input=(webB)
input=(as webB wikiT lj enwiki-2013 orkut)
querys=(threeTriangle)
# querys=(chordalSquare)

for i in ${input[@]}; do
	data=$i
	file="${prefix}/${data}_undir"
	for query in ${querys[@]}; do
			echo "----------------------------------"
			echo executing $i $query

			SECONDS=0
			runSpark-logo.sh  --num-executors 32  --class $mainClass $JAR $file $query 3

			duration=$SECONDS
			echo "executing $i $duration seconds elapsed."
	done
done
querys=()

# input=(wikiT lj enwiki-2013 orkut)
# querys=(threeTriangle)
# input=(webB as wikiT lj)
# input=(webB)
# querys=(fourCliqueEdge twoHeadTriangle)
for i in ${input[@]}; do
	data=$i
	file="${prefix}/${data}_undir"
	for query in ${querys[@]}; do
			echo "----------------------------------"
			echo executing $i $query

			SECONDS=0
			runSpark-logo.sh  --num-executors 32  --class $mainClass $JAR $file $query 3

			duration=$SECONDS
			echo "executing $i $duration seconds elapsed."
	done
done
querys=()

# input=(wikiT lj enwiki-2013 orkut)
# querys=(near5Clique)

for i in ${input[@]}; do
	data=$i
	file="${prefix}/${data}_undir"
	for query in ${querys[@]}; do
			echo "----------------------------------"
			echo executing $i $query

			SECONDS=0
			runSpark-logo.sh  --num-executors 32  --class $mainClass $JAR $file $query 3

			duration=$SECONDS
			echo "executing $i $duration seconds elapsed."
	done
done
}

TestCommScalability(){

echo "test communication scalability"

mainClass=hzhang.framework.test.exp.entry.CommExp
JAR="./Logo-assembly-1.2.6.jar"

input=(gen5 gen10 gen20 gen40 gen80 gen160 gen320)
# input=(gen80)
# querys=(hyberCubeTriangle hyberCubeFourClique triangle fourClique)
# querys=(chordalSquare)

for i in ${input[@]}; do
	data=$i
	file="${prefix}/${data}_undir"

	echo "----------------------------------"
	echo executing $i $query

	SECONDS=0
	# runSpark-logo.sh  --num-executors 32  --class $mainClass $JAR $root "hyberCubeTriangle" 10

	# runSpark-logo.sh  --num-executors 32  --class $mainClass $JAR $root "triangle" 10

	runSpark-logo.sh  --num-executors 32  --class $mainClass $JAR $file "hyberCubeFourClique" 10

	# runSpark-logo.sh  --num-executors 32  --class $mainClass $JAR $root "fourClique" 10

	duration=$SECONDS
	echo "executing $i $duration seconds elapsed."
done
}

TestWorkloadBalance(){
echo "test workload balance"

mainClass=hzhang.framework.test.exp.entry.ADJExp
JAR="./Logo-assembly-1.2.6.jar"


input=(webB)
# querys=(house threeTriangle near5Clique)
querys=(threeTriangle near5Clique)
num_work=(8)
# num_work=(6)
for i in ${input[@]}; do
		data=$i
		file="${prefix}/${data}_undir"

		for query in ${querys[@]}; do
			for h in ${num_work[@]};do
				echo "----------------------------------"
				echo executing $i $query $k

				SECONDS=0
				runSpark-logo-oneCore.sh --class $mainClass $JAR $file $query $h false

				duration=$SECONDS
				echo "executing $i $duration seconds elapsed."
			done
		done
done
}

gen_edge(){
echo "generate relations"

mainClass=hzhang.framework.test.exp.entry.ADJExp
JAR="./Logo-assembly-1.2.6.jar"
}


Test_HCUBEGJ
#Test_PatternStatistic
#TestCommScalability
#TestWorkloadBalance
# Test_Speed
# TestEager
# TestGJ
# TestForLoop


