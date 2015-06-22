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

package partitioner;

import architecture.Network;
import core.Edge;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Partitioner {
    
    private LinkedList<Edge> dataset;
    private PartitionState state;
    private Network net;
    private PartitionStrategy algorithm;

    public Partitioner(LinkedList<Edge> dataset, Network net) {
        this.dataset = dataset;
        this.net = net;
        state = new PartitionState();
        if (net.GLOBALS.PARTITION_STRATEGY.equalsIgnoreCase("greedy")){ algorithm = new Greedy(); }
        else if (net.GLOBALS.PARTITION_STRATEGY.equalsIgnoreCase("hashing")){ algorithm = new Hashing(); }
        else if (net.GLOBALS.PARTITION_STRATEGY.equalsIgnoreCase("grid")){ algorithm = new Grid(net.GLOBALS); }
        else if (net.GLOBALS.PARTITION_STRATEGY.equalsIgnoreCase("hdrf")){ algorithm = new HDRF(); }
    }  
    
    public void performPartition(){
        if (net.GLOBALS.LOAD_FROM_DISK){
            try{
                //FAST ACCESS TO OBJECT IN STORAGE
                FileInputStream fileIn =new FileInputStream(net.GLOBALS.PARTITION_STATE_OBJECT_PATH);  
                ObjectInputStream in = new ObjectInputStream(fileIn);  
                state = (PartitionState) in.readObject();  
                state.notifyMasters(net);
                state.loadMachineEdges(net);
                in.close();  
                fileIn.close();
            }catch(Exception e1){
                System.out.println("Partition state deserialization Exception "+e1);
                try{
                    start();
                    //STORE THE OBJECTIN STORAGE FOR FAST ACCESS
                    FileOutputStream fileOut = new FileOutputStream(net.GLOBALS.PARTITION_STATE_OBJECT_PATH);  
                    ObjectOutputStream outStream = new ObjectOutputStream(fileOut);  
                    outStream.writeObject(state);  
                    state.storeMachineEdges(net);
                    outStream.close();  
                    fileOut.close();  
                }catch(Exception e2){
                    System.out.println("Partition state serialization Exception "+e2);
                    e2.printStackTrace();
                    System.exit(-1);
                }
            }
        }
        else{ start(); }
    }
    
    private void start(){
        int processors = Runtime.getRuntime().availableProcessors();
        ExecutorService executor=Executors.newFixedThreadPool(processors);
        
        //Collections.shuffle(dataset); //random shuffling
        
        int n = dataset.size();
        int subSize = n / processors + 1;
        for (int t = 0; t < processors; t++) {
            final int iStart = t * subSize;
            final int iEnd = Math.min((t + 1) * subSize, n);
            if (iEnd>=iStart){
                List<Edge> list= dataset.subList(iStart, iEnd);
                Runnable x = new PartitionerThread(list, state, net, algorithm);
                executor.execute(x);
            }
        }
        try { 
            executor.shutdown();
            executor.awaitTermination(60, TimeUnit.DAYS);
        } catch (InterruptedException ex) {System.out.println("InterruptedException "+ex);ex.printStackTrace();}
    }   

    public PartitionState getState() {
        return state;
    }
    
}
