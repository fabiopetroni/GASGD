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
import architecture.Network;
import core.Edge;
import input.Input;
import input.YahooInput;
import java.util.Collections;
import java.util.LinkedList;
import output.Statistics;
import partitioner.PartitionState;
import partitioner.Partitioner;
import sgd.DSGD;

public class Main {
    public static void main(String[] args) {
        
        long TIME_0 = System.currentTimeMillis();
        
        System.out.println("\n------------------------------------------------------------");
        System.out.println(" GASGD: Distributed Asynchronous Matrix Completion Simulator.");
        System.out.println(" author: Fabio Petroni (http://www.fabiopetroni.com)");
        System.out.println("------------------------------------------------------------\n");
        Globals GLOBALS = new Globals(args);
        System.out.println("    Parameters:");
        GLOBALS.print();
        System.out.print("\n Loading graph into main memory... ");
        //YahooInput input = new YahooInput(GLOBALS);
        Input input = new Input(GLOBALS);        
         
        
        //Shuffle the dataset for better performance
        LinkedList<Edge> dataset = input.getDataset();
        Collections.shuffle(dataset);
        
        long TIME_1 = System.currentTimeMillis();        
        long time = TIME_1-TIME_0;
        time /= 1000; //sec
        System.out.println((int) time +" seconds");         
        
        Statistics stat = new Statistics(GLOBALS);
        Network net = new Network(GLOBALS,stat);

        System.out.print("\n Partitioning the input... ");

        Partitioner p = new Partitioner(dataset,net);
        p.performPartition();
        PartitionState state = p.getState();
        stat.computeReplicationFactor(state);  
        stat.computeStdDevLoad(net);
        double std_dev = Math.floor(stat.getStdDevLoad() * 100)/100;

        long TIME_2 = System.currentTimeMillis();
        time = TIME_2-TIME_1;
        time /= 1000; //sec
        System.out.println((int) time +" seconds"); 
        
        System.out.println("     Results:");
        System.out.println("\tReplication factor: "+stat.getReplicationFactor());
        System.out.println("\tLoad relative standard deviation: "+std_dev);

        System.out.println("\n Running Asynchronous Stochastic Gradient Descent... ");
        
        DSGD sgd = new DSGD(net, state, stat);
        double[] losses = sgd.perform();

        long TIME_3 = System.currentTimeMillis();
        time = TIME_3-TIME_2;
        time /= 1000; //sec
        System.out.println("     ..."+(int) time +" seconds"); 

        stat.setSTART_TIME(TIME_1);
        stat.setMEDIUM_TIME(TIME_2);
        stat.setEND_TIME(TIME_3);

        if (GLOBALS.PRINT_RESULT){stat.print(losses);}
    }
}
