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
import application.Globals;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Random;
import java.util.TreeSet;
import local.Machine;

/**
 * Greedy Vertex-Cuts
 */
public class Greedy implements PartitionStrategy{

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
        
        int machine_id = -1;
        
        //Case 4: If neither vertex has been assigned, then assign the edge to the least loaded machine.
        if (!user_record.hasMaster() && !item_record.hasMaster()){ 
            LinkedList<Integer> candidates = new LinkedList<Integer>();
            int min_load = Integer.MAX_VALUE;
            for (int m = 0; m<P; m++){
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
            machine_id = candidates.get(choice);            
            
            Machine x = net.getMachine(machine_id);
            
            //1-UPDATE RECORDS
            user_record.setMaster(machine_id);
            item_record.setMaster(machine_id);
            
            //2-UPDATE EDGES
            x.addEdge(t);
            
            //3- UPDATE ROUTING TABLE
            x.addMasterItem(item);
            x.addMasterUser(user);
        }
                
        //Case 3: If only one of the two vertices has been assigned, then choose a machine from the assigned vertex.
        else if (!user_record.hasMaster()){            
            int item_master = item_record.getMaster();
            int min_load = net.getMachine(item_master).getEdgesNumber();
            
            LinkedList<Integer> candidates = new LinkedList<Integer>();
            candidates.add(item_master);
            
            Iterator<Byte> it = item_record.getSlave();
            while(it.hasNext()){
                int m = it.next();
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
            machine_id = candidates.get(choice);            
            
            Machine x = net.getMachine(machine_id);
            
            //1-UPDATE RECORDS
            user_record.setMaster(machine_id);
            
            //2-UPDATE EDGES
            x.addEdge(t);
            
            //3- UPDATE ROUTING TABLE
            x.addMasterUser(user);
        }
        
        else if (!item_record.hasMaster()){
            int user_master = user_record.getMaster();
            int min_load = net.getMachine(user_master).getEdgesNumber();
            
            LinkedList<Integer> candidates = new LinkedList<Integer>();
            candidates.add(user_master);
            
            Iterator<Byte> it = user_record.getSlave();
            while(it.hasNext()){
                int m = it.next();
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
            machine_id = candidates.get(choice);
            
            Machine x = net.getMachine(machine_id);
            
            //1-UPDATE RECORDS
            item_record.setMaster(machine_id);
            
            //2-UPDATE EDGES
            x.addEdge(t);
            
            //3- UPDATE ROUTING TABLE
            x.addMasterItem(item);
        }
                
        else{
            TreeSet<Byte> intersection = Record.intersection(user_record, item_record);
            
            //Case 1: If A(u) and A(v) intersect, then the edge should be assigned to a machine in the intersection.
            if (!intersection.isEmpty()){
                LinkedList<Integer> candidates = new LinkedList<Integer>();
                int min_load = Integer.MAX_VALUE;
                
                for (int m : intersection){
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
                machine_id = candidates.get(choice);
                
                Machine x = net.getMachine(machine_id);

                //2-UPDATE EDGES
                x.addEdge(t);
            }
            
            //Case 2: If A(u) and A(v) are not empty and do not intersect, then the edge should be assigned to one of the machines from the vertex with the most unassigned edges.
            else{              
                
                int item_master = item_record.getMaster();
                int item_min_load = net.getMachine(item_master).getEdgesNumber();                
                LinkedList<Integer> candidates_item = new LinkedList<Integer>();
                candidates_item.add(item_master);                
                Iterator<Byte> it = item_record.getSlave();
                while(it.hasNext()){
                    int m = it.next();
                    int load = net.getMachine(m).getEdgesNumber();
                    if (load<item_min_load){
                        candidates_item.clear();
                        item_min_load = load;
                        candidates_item.add(m);
                    }
                    if (load == item_min_load){
                        candidates_item.add(m);
                    }
                }    
                
                int user_master = user_record.getMaster();
                int user_min_load = net.getMachine(user_master).getEdgesNumber();
                LinkedList<Integer> candidates_user = new LinkedList<Integer>();
                candidates_user.add(user_master);                
                it = user_record.getSlave();
                while(it.hasNext()){
                    int m = it.next();
                    int load = net.getMachine(m).getEdgesNumber();
                    if (load<user_min_load){
                        candidates_user.clear();
                        user_min_load = load;
                        candidates_user.add(m);
                    }
                    if (load == user_min_load){
                        candidates_user.add(m);
                    }
                }
                
                if (net.GLOBALS.partitioned==Globals.PARTITIONED.USER){
                    
                    //*** PICK A RANDOM ELEMENT FROM CANDIDATES USER
                    Random r = new Random(); 
                    int choice = r.nextInt(candidates_user.size());
                    machine_id = candidates_user.get(choice);
                    
                    Machine slave_machine = net.getMachine(machine_id);
            
                    //1-UPDATE RECORDS
                    item_record.addSlave(machine_id);

                    //2-UPDATE EDGES
                    slave_machine.addEdge(t);

                    //3- UPDATE ROUTING TABLE OF SLAVE_MACHINE AND MASTER_MACHINE
//                    int master = item_record.getMaster();
//                    Machine master_machine = net.getMachine(master);
                }
                else if (net.GLOBALS.partitioned==Globals.PARTITIONED.ITEM){
                    //*** PICK A RANDOM ELEMENT FROM CANDIDATES ITEM
                    Random r = new Random(); 
                    int choice = r.nextInt(candidates_item.size());
                    machine_id = candidates_item.get(choice);                    
                    
                    Machine slave_machine = net.getMachine(machine_id);
            
                    //1-UPDATE RECORDS
                    user_record.addSlave(machine_id);

                    //2-UPDATE EDGES
                    slave_machine.addEdge(t);

                    //3- UPDATE ROUTING TABLE OF SLAVE_MACHINE AND MASTER_MACHINE
//                    int master = user_record.getMaster();
//                    Machine master_machine = net.getMachine(master);
                }
                else{   
                    
                    //RANDOM BREAK IF SIMMETRY OCCURS
                    if (user_min_load == item_min_load){
                        if (Math.random() < 0.5) user_min_load++;
                        else item_min_load++;
                    }                       
                        
                    if (user_min_load < item_min_load){
                        //*** PICK A RANDOM ELEMENT FROM CANDIDATES USER
                        Random r = new Random(); 
                        int choice = r.nextInt(candidates_user.size());
                        machine_id = candidates_user.get(choice);
                    
                        Machine slave_machine = net.getMachine(machine_id);

                        //1-UPDATE RECORDS
                        item_record.addSlave(machine_id);

                        //2-UPDATE EDGES
                        slave_machine.addEdge(t);

                        //3- UPDATE ROUTING TABLE OF SLAVE_MACHINE AND MASTER_MACHINE
//                        int master = item_record.getMaster();
//                        Machine master_machine = net.getMachine(master);
    //                    slave_machine.addSlaveItem(item, master);
    //                    master_machine.addMasterItemSlave(item, machine_id);
                    }
                    else if (item_min_load < user_min_load){
                        //*** PICK A RANDOM ELEMENT FROM CANDIDATES ITEM
                        Random r = new Random(); 
                        int choice = r.nextInt(candidates_item.size());
                        machine_id = candidates_item.get(choice);  
                    
                        Machine slave_machine = net.getMachine(machine_id);

                        //1-UPDATE RECORDS
                        user_record.addSlave(machine_id);

                        //2-UPDATE EDGES
                        slave_machine.addEdge(t);

                        //3- UPDATE ROUTING TABLE OF SLAVE_MACHINE AND MASTER_MACHINE
//                        int master = user_record.getMaster();
//                        Machine master_machine = net.getMachine(master);
    //                    slave_machine.addSlaveUser(user, master);
    //                    master_machine.addMasterUserSlave(user, machine_id);      
                    }  
                    else{
                        System.out.println("Logic error greedy");
                        System.exit(-1); 
                    }
                }
            }
        }
        
        //*** RELEASE LOCK
        user_record.releaseLock();
        item_record.releaseLock();
    }

}
