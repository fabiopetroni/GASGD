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

import core.Edge;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Random;
import application.Globals;
import architecture.Network;
import local.Machine;

public class Constrained implements PartitionStrategy{
    
    public static final int MAX_SHRINK = 100;
    double seed;
    int shrink;
    int partitions;
    int nrows, ncols;
    LinkedList<Integer>[] constraint_graph;
    private Globals GLOBALS;
    
    public Constrained(Globals G){
        this.seed = Math.random();
        Random r = new Random(); 
        shrink = r.nextInt(MAX_SHRINK);
        this.GLOBALS = G;
        this.partitions = this.GLOBALS.P;
        this.constraint_graph = new LinkedList[this.partitions];
        if (this.GLOBALS.PARTITION_STRATEGY.equalsIgnoreCase("grid")) {
            make_grid_constraint();
        } else if (this.GLOBALS.PARTITION_STRATEGY.equalsIgnoreCase("pds")) {
            make_pds_constraint();
        }
    }
    
    private void make_grid_constraint() {
        initializeRowColGrid();
        for (int i = 0; i < partitions; i++) {
            LinkedList<Integer> adjlist = new LinkedList<Integer>();
            // add self
            adjlist.add(i);
            // add the row of i
            int rowbegin = (i/ncols) * ncols;
            for (int j = rowbegin; j < rowbegin + ncols; ++j)
              if (i != j) adjlist.add(j);
            // add the col of i
            for (int j = i % ncols; j < partitions; j+=ncols){
                  if (i != j) adjlist.add(j);
            }
            Collections.sort(adjlist);
            constraint_graph[i]=adjlist;
        }
        //DEBUG
//        for (int i = 0; i < partitions; i++) {
//            System.out.print(i+" --> [ ");
//            for (int j: constraint_graph[i]){
//                System.out.print(j+" ");
//            }
//            System.out.println("]");
//        }
    }
    
    private void initializeRowColGrid() {
        double approx_sqrt = Math.sqrt(partitions);
        nrows = (int) approx_sqrt;
        for (ncols = nrows; ncols <= nrows + 2; ++ncols) {
            if (ncols * nrows == partitions) {
                return;
            }
        }
        System.out.println("ERRORE Num partitions "+partitions+" cannot be used for grid ingress.");
        System.exit(-1);
    }
    
    private void make_pds_constraint() {
        int p = initializeRowColPds();
        Pds pds_generator = new Pds();
        LinkedList<Integer> results = new LinkedList<Integer>();
        if (p == 1) {
            results.add(0);
            results.add(2);
        } else {
            results = pds_generator.get_pds(p);
        }
        for (int i = 0; i < partitions; i++) {
            LinkedList<Integer> adjlist = new LinkedList<Integer>();
            for (int j = 0; j < results.size(); j++) {
                adjlist.add( (results.get(j) + i) % partitions);
            }
            Collections.sort(adjlist);
            constraint_graph[i]=adjlist;
        }
//        //DEBUG
//        for (int i = 0; i < partitions; i++) {
//            System.out.print(i+" --> [ ");
//            for (int j: constraint_graph[i]){
//                System.out.print(j+" ");
//            }
//            System.out.println("]");
//        }
    }
    
    private int initializeRowColPds() {
        int p = (int) Math.sqrt(partitions-1);
        if (!(p>0 && ((p*p+p+1) == partitions))){
            System.out.println("ERRORE Num partitions "+partitions+" cannot be used for pds ingress.");
            System.exit(-1);
        }
        return p;
    }    
    
    @Override
    public void performStep(Edge t, PartitionState state, Network net) {
        int P = net.GLOBALS.P;
        int user = t.user;
        int item = t.item;
        
        Record user_record = state.getUserRecord(user);
        Record item_record = state.getItemRecord(item);
        
        //*** ASK FOR LOCK
        int sleep = 2; while (!user_record.getLock()){ try{ Thread.sleep(sleep); }catch(Exception e){} sleep = (int) Math.pow(sleep, 2);}
        sleep = 2; while (!item_record.getLock()){ try{ Thread.sleep(sleep); }catch(Exception e){} sleep = (int) Math.pow(sleep, 2); 
        if (sleep>net.GLOBALS.SLEEP_LIMIT){user_record.releaseLock(); performStep(t,state,net); return;} //TO AVOID DEADLOCK
        }
        //*** LOCK TAKEN
        
        int shard_user = Math.abs((int) ( (int) user*seed*shrink) % P);  
        int shard_item = Math.abs((int) ( (int) item*seed*shrink) % P);  
        
        LinkedList<Integer> costrained_set = (LinkedList<Integer>) constraint_graph[shard_user].clone();
        costrained_set.retainAll(constraint_graph[shard_item]);
        
        //CASE 1: GREEDY ASSIGNMENT
        LinkedList<Integer> candidates = new LinkedList<Integer>();
        int min_load = Integer.MAX_VALUE;
        for (int m : costrained_set){
            int load = net.getMachine(m).getEdgesNumber();
            if (load<min_load){
                candidates.clear();
                min_load = load;
                candidates.add(m);
            }
            if (load == min_load){
                candidates.add(m);
            }
        }
        //*** PICK A RANDOM ELEMENT FROM CANDIDATES
        Random r = new Random(); 
        int choice = r.nextInt(candidates.size());
        int machine_id = candidates.get(choice);      
        
        //CASE 2 : RANDOM ASSIGNMENT
//        Random r = new Random(); 
//        int choice = r.nextInt(costrained_set.size());
//        int machine_id = costrained_set.get(choice);
        
        //System.out.println(machine_id);
        Machine x = net.getMachine(machine_id);
        
        //1-UPDATE EDGES
        x.addEdge(t);

        if (!user_record.hasMaster()){
            //a-UPDATE RECORDS
            user_record.setMaster(machine_id);
            //b- UPDATE ROUTING TABLE
            x.addMasterUser(user); 
        }
        else if (!user_record.contains(machine_id)){
            //a-UPDATE RECORDS
            user_record.addSlave(machine_id);
        }
        
        if (!item_record.hasMaster()){
            //a-UPDATE RECORDS
            item_record.setMaster(machine_id);
            //b- UPDATE ROUTING TABLE
            x.addMasterItem(item);
        }
        else if (!item_record.contains(machine_id)){
            //a-UPDATE RECORDS
            item_record.addSlave(machine_id);
        }
          
        //*** RELEASE LOCK
        user_record.releaseLock();
        item_record.releaseLock();
    }
}
