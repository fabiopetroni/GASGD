// Copyright (C) 2015 Fabio Petroni
// Contact:   http://www.fabiopetroni.com
//
// This file is part of GASGD simulator.
//
// GASGD is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// GASGD is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with GASGD.  If not, see <http://www.gnu.org/licenses/>.
//
// Based on the publication:
// - Fabio Petroni and Leonardo Querzoni (2014): GASGD: stochastic gradient descent for  
//   distributed asynchronous matrix completion via graph partitioning.
//   In Proceedings of the 8th ACM Conference on Recommender systems (RecSys), 2014.

package application;

public class Globals {
    public static enum GENDER{USER, ITEM};
    public static enum PARTITIONED{ITEM, USER};
    
    //mandatory SIMULATOR PARAMETERS
    public int P;  //number of machines
    public String INPUT_FILE_NAME;    
    
    //optional SIMULATOR PARAMETERS
    public String INPUT_FILE_SEPARATOR = "\t";    
    public int Frequency = 1; //How many times the machines comunicate during each epoch
    public String PARTITION_STRATEGY = "greedy"; // hdrf, greedy, hashing, grid
    public PARTITIONED partitioned = null;    
    
    //optional SGD PARAMETERS
    public int K = 50; //hidden features
    public double Lambda = 0.05; //regularization factor (0.05 NETFLIX, 1 YAHOO)
    public double init_mu = 0.01; //lerning rate   (0.015 NETFLIX, 0.0001 YAHOO)
    public int iterations = 30;
    public boolean COMPUTE_LOSS = true;
    
    //optional OUTPUT definition
    public boolean PRINT_RESULT = false;
    public String OUTPUT_DIR;
    public String OUTPUT_LOSS_FILE;
    public String OUTPUT_COMMUNICATION_COST_FILE;
    public String OUTPUT_INFO_FILE;
    
    //CONSTANT VALUES:
    public int SLEEP_LIMIT = 1024;
    
    public int NUM_USERS;
    public int NUM_ITEMS;
    //...headers
    public final static String INIT_HEADER = "init";
    public final static String SLAVE_PROFILE_HEADER = "slave";
    public final static String MASTER_PROFILE_HEADER = "master";
    //...objecti on storage path
    public boolean LOAD_FROM_DISK = false; 
    public String PARTITION_STATE_OBJECT_PATH = PARTITION_STRATEGY+"_"+partitioned+".state";
    public String MACHINE_STORED_EDGES_PATH = PARTITION_STATE_OBJECT_PATH+"_"+partitioned+"."+P+"machine";
    public String DATASET_OBJECT_PATH = INPUT_FILE_NAME+".dataset";   
    
    
    public Globals(String[] args){
        parse_arguments(args);
    }
    
    private void parse_arguments(String[] args){
        try{
            INPUT_FILE_NAME = args[0];
            P = Integer.parseInt(args[1]);   
            for(int i=2; i < args.length; i+=2){
                if(args[i].equalsIgnoreCase("-separator")){
                    INPUT_FILE_SEPARATOR = args[i+1];
                }
                else if(args[i].equalsIgnoreCase("-frequency")){
                    Frequency = Integer.parseInt(args[i+1]);
                }
                else if(args[i].equalsIgnoreCase("-partitioned")){
                    if (args[i+1].equals("user")){
                        partitioned = PARTITIONED.USER;
                    }
                    else if (args[i+1].equals("item")){
                        partitioned = PARTITIONED.ITEM;
                    }
                    else{
                        System.out.println("\nInvalid option "+args[i+1]+" for the greedy algorithm. Aborting.");
                        System.out.println("Valid option = 'user' or 'item'\n");
                        System.exit(-1);
                    }                        
                }
                else if(args[i].equalsIgnoreCase("-partitioning_algorithm")){
                    PARTITION_STRATEGY =args[i+1];
                    if (PARTITION_STRATEGY.equalsIgnoreCase("greedy")){}
                    else if (PARTITION_STRATEGY.equalsIgnoreCase("hdrf")){}
                    else if (PARTITION_STRATEGY.equalsIgnoreCase("hashing")){}
                    else if (PARTITION_STRATEGY.equalsIgnoreCase("grid")){}
                    else{
                        System.out.println("\nInvalid algorithm "+PARTITION_STRATEGY+". Aborting.");
                        System.out.println("Valid algorithms: hdrf, greedy, hashing, grid\n");
                        System.exit(-1);
                    }
                }
                else if(args[i].equalsIgnoreCase("-output_dir")){
                    OUTPUT_DIR = args[i+1];
                    OUTPUT_LOSS_FILE = OUTPUT_DIR+"/"+P+"_"+PARTITION_STRATEGY+"_loss.dat";
                    OUTPUT_COMMUNICATION_COST_FILE = OUTPUT_DIR+"/"+P+"_"+PARTITION_STRATEGY+"_communicationcost.dat";
                    OUTPUT_INFO_FILE = OUTPUT_DIR+"/"+P+"_"+PARTITION_STRATEGY+"_info.dat";
                    PRINT_RESULT = true;
                }
                else if(args[i].equalsIgnoreCase("-iterations")){
                    iterations = Integer.parseInt(args[i+1]);
                }
                else if(args[i].equalsIgnoreCase("-lambda")){
                    Lambda = Double.parseDouble(args[i+1]);
                }
                else if(args[i].equalsIgnoreCase("-learning_rate")){
                    init_mu = Double.parseDouble(args[i+1]);
                }
                else if(args[i].equalsIgnoreCase("-rank")){
                    K = Integer.parseInt(args[i+1]);
                }
                else throw new IllegalArgumentException();
            }
        } catch (Exception e){
            System.out.println("\nInvalid arguments ["+args.length+"]. Aborting.\n");
            System.out.println("Usage:\n GASGD inputfile nmachines [options]\n");
            System.out.println("Parameters:");
            System.out.println(" inputfile: the name of the file that stores the <user,item,rating> triples.");
            System.out.println(" nmachines: the number of machines for the simulated cluster. Maximum value 256.");
            System.out.println("\nOptions:");
            System.out.println(" -separator string");
            System.out.println("\t specifies the separator between user, item and rating in the input file . Default '\\t'.");
            System.out.println(" -partitioning_algorithm string");
            System.out.println("\t specifies the algorithm to be used by the input partitioner procedure (hdrf greedy hashing grid). Default greedy.");
            System.out.println(" -frequency integer");
            System.out.println("\t specifies how many times the machines comunicate during each epoch. Default 1.");
            System.out.println(" -partitioned string");
            System.out.println("\t specifies if the greedy algorithm is bipartite aware, partitioning 'user' or 'item' respectively. Default null.");
            System.out.println(" -output_dir string");
            System.out.println("\t specifies the name of the directory where the output files will be stored.");
            System.out.println(" -iterations integer");
            System.out.println("\t specifies how many iterations to be performed by the sgd algorithm. Default 30.");
            System.out.println(" -lambda double");
            System.out.println("\t specifies the regularization parameter for the sgd algorithm. Default 0.05.");
            System.out.println(" -learning_rate double");
            System.out.println("\t specifies the learning rate for the sgd algorithm. Default 0.01.");
            System.out.println(" -rank integer");
            System.out.println("\t specifies the number of latent features for the low rank approximation. Default 50.");
            System.out.println();
            System.exit(-1);
        }
    }
    
    public void print(){
        System.out.println("\tinputfile: "+INPUT_FILE_NAME);
        System.out.println("\tnmachines: "+P);
        System.out.print("\tpartitioning_algorithm: "+PARTITION_STRATEGY);
        if (PARTITION_STRATEGY.equalsIgnoreCase("greedy")){ System.out.println(" (partitioned: "+partitioned+")"); }
        else System.out.println("");
        System.out.println("\tseparator: '"+INPUT_FILE_SEPARATOR+"'");
        System.out.println("\tfrequency: "+Frequency);
        System.out.println("\toutput_dir: "+OUTPUT_DIR);
        System.out.println("\titerations: "+iterations);
        System.out.println("\tlambda: "+Lambda);
        System.out.println("\tlearning_rate: "+init_mu);
        System.out.println("\trank: "+K);
    }

    public void setNUM_USERS(int NUM_USERS) {
        this.NUM_USERS = NUM_USERS;
    }

    public void setNUM_ITEMS(int NUM_ITEMS) {
        this.NUM_ITEMS = NUM_ITEMS;
    }
}
