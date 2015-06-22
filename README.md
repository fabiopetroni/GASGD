# GASGD
Distributed Asynchronous Matrix Completion Simulator

A simulator for Distributed Asynchronous Matrix Completion.
 Based on the publication:

-  F. Petroni and L. Querzoni:
   GASGD: stochastic gradient descent for distributed asynchronous matrix completion via graph partitioning.
   In: Proceedings of the 8th ACM Conference on Recommender systems (RecSys), 2014.
   
Abstract:  Matrix completion latent factors models are known to be an effective method to build recommender systems. Currently, stochastic gradient descent (SGD) is considered one of the best latent factor-based algorithm for matrix completion. In this paper we discuss GASGD, a distributed asynchronous variant of SGD for large-scale matrix completion, that (i) leverages data partitioning schemes based on graph partitioning techniques, (ii) exploits specific characteristics of the input data and (iii) introduces an explicit parameter to tune synchronization frequency among the computing nodes.
We empirically show how, thanks to these features, GASGD achieves a fast convergence rate incurring in smaller communication cost with respect to current asynchronous distributed SGD implementations. 

To run the project from the command line, go to the dist folder and type the following:

java -Xmx§GB -jar GASGD.jar inputfile nmachines [options]

Parameters:
 - §: number of GB for the java virtual machine
 - inputfile: the name of the file that stores the <user,item,rating> triples.
 - nmachines: the number of machines for the simulated cluster. Maximum value 256.

Options:
 - -separator string \t
	 specifies the separator between user, item and rating in the input file . Default '\t'.
 - -partitioning_algorithm string \t
	 specifies the algorithm to be used by the input partitioner procedure (hdrf greedy hashing grid). Default greedy.
 - -frequency integer \t
	 specifies how many times the machines comunicate during each epoch. Default 1.
 - -partitioned string \t
	 specifies if the greedy algorithm is bipartite aware, partitioning 'user' or 'item' respectively. Default null.
 - -output_dir string \t
	 specifies the name of the directory where the output files will be stored.
 - -iterations integer \t
	 specifies how many iterations to be performed by the sgd algorithm. Default 30.
 - -lambda double \t
	 specifies the regularization parameter for the sgd algorithm. Default 0.05.
 - -learning_rate double \t
	 specifies the learning rate for the sgd algorithm. Default 0.01.
 - -rank integer \t
	 specifies the number of latent features for the low rank approximation. Default 50.

