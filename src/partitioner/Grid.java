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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import local.Machine;

public class Grid implements PartitionStrategy{
    
    int m_items;
    int m_users;
    int NUM_USERS;
    int NUM_ITEMS;
    int step_items;
    int step_users;
    int partitions;
    HashMap<Integer,Integer> conversion_users;
    HashMap<Integer,Integer> conversion_items;
    
    public Grid(Globals G){
        this.NUM_USERS = G.NUM_USERS;
        this.NUM_ITEMS = G.NUM_ITEMS;
        this.partitions = G.P;
        factorize();
        buildRandomMap();
    }
    
    private static List<Integer> primeFactors(int number) {
        int n = number;
        List<Integer> factors = new ArrayList<Integer>();
        for (int i = 2; i <= n; i++) {
            while (n % i == 0) {
                factors.add(i);
                n /= i;
            }
        }
        return factors;
    }

    private synchronized void factorize()  { 
        m_items = 1; //item
        m_users = 1; //users
        List<Integer> prime_factors = primeFactors(partitions);
        if (prime_factors.size()<2){return;} 
        int c = prime_factors.size() / 2;
        int i = 0;
        for (Integer integer : prime_factors) {
            if (i<c){ m_items *= integer; }
            else{ m_users *= integer; }
            i++;
        }
        step_items = NUM_ITEMS / m_items;
        step_users = NUM_USERS / m_users;
        System.out.println("m_items: "+m_items);
        System.out.println("m_users: "+m_users);
        System.out.println("step_items: "+step_items);
        System.out.println("step_users: "+step_users);
    } 
    
    private synchronized int getCluster(int user_id,int item_id){
        //System.out.println("\n");
        int p_user = getPuser(user_id);
        //System.out.println("p_user: "+p_user);
        int p_item = getPitem(item_id);
        //System.out.println("p_item: "+p_item);
        int cluster = (p_user*m_items);
        //System.out.println("p_user*m_items: "+cluster);
        cluster += p_item;
        //System.out.println("cluster: "+cluster);
        return cluster;
    }
    
    private synchronized int getPuser(int user_id){
        if (!conversion_users.containsKey(user_id)){
            System.out.println("user "+user_id+" not present in conversion map!");
            System.exit(-1);
        }
        int random_user_id = conversion_users.get(user_id);
        int x = random_user_id / step_users;
        //System.out.println("user_id: "+user_id);
        //System.out.println("random_user_id: "+random_user_id);
        return x % m_users;
    }
    
    private synchronized int getPitem(int item_id){
        if (!conversion_items.containsKey(item_id)){
            System.out.println("item "+item_id+" not present in conversion map!");
            System.exit(-1);
        }
        int random_item_id = conversion_items.get(item_id);
        int x = random_item_id / step_items;
        //System.out.println("item_id: "+item_id);
        //System.out.println("random_item_id: "+random_item_id);
        return x % m_items;
    }
    
    
    private synchronized void buildRandomMap(){
        conversion_users = new HashMap<Integer,Integer>();
        conversion_items = new HashMap<Integer,Integer>();
        LinkedList<Integer> aux = new LinkedList<Integer>();
        //USERS
        for (int i = 0; i< NUM_USERS; i++){
            aux.add(i);
        }
        int u = 0;
        while (!aux.isEmpty()){
            int size = aux.size();
            int index = new Random().nextInt(size);
            int x = aux.get(index);
            conversion_users.put(x, u);
            aux.remove(index);
            u++;
        }
        //ITEMS
        for (int j = 0; j< NUM_ITEMS; j++){
            aux.add(j);
        }
        int i = 0;
        while (!aux.isEmpty()){
            int size = aux.size();
            int index = new Random().nextInt(size);
            int x = aux.get(index);
            conversion_items.put(x, i);
            aux.remove(index);
            i++;
        }
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
        
        int machine_id = getCluster(user,item); 
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
