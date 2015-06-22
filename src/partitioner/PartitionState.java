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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.HashMap;
import local.Machine;

public class PartitionState implements Serializable{
    private HashMap<Integer,Record> user_map;
    private HashMap<Integer,Record> item_map;

    public PartitionState() {
        user_map = new HashMap<Integer,Record>();
        item_map = new HashMap<Integer,Record>(); 
    }

    public synchronized Record getUserRecord(int user){
        if (!user_map.containsKey(user)){
            user_map.put(user, new Record());
        }
        return user_map.get(user);
    }
    
    public synchronized Record getItemRecord(int item){
        if (!item_map.containsKey(item)){
            item_map.put(item, new Record());
        }
        return item_map.get(item);
    }
    
    public int getItemNumber(){
        return item_map.size();
    }
    
    public int getUserNumber(){
        return user_map.size();
    }
    
    public int getSumItemsCopies(){
        int result = 0;
        for (int x : item_map.keySet()){
            result += item_map.get(x).getCopies();
        }
        return result;
    }
    
    public int getSumUsersCopies(){
        int result = 0;
        for (int x : user_map.keySet()){
            result += user_map.get(x).getCopies();
        }
        return result;
    }
    
    protected void notifyMasters(Network net){
        for (int x : item_map.keySet()){
            Record r = item_map.get(x);
            int master = r.getMaster();
            net.getMachine(master).addMasterItem(x);
        }
        for (int x : user_map.keySet()){
            Record r = user_map.get(x);
            int master = r.getMaster();
            net.getMachine(master).addMasterUser(x);
        }
    }
    
    protected void loadMachineEdges(Network net) throws FileNotFoundException, IOException, ClassNotFoundException{
        for (int m=0; m<net.GLOBALS.P; m++){
            Machine machine = net.getMachine(m);
            machine.loadEdges();
        }
    }
    
    protected void storeMachineEdges(Network net) throws FileNotFoundException, IOException{
        for (int m=0; m<net.GLOBALS.P; m++){
            Machine machine = net.getMachine(m);
            machine.storeEdges();
        }
    }
    
    
    private void writeObject(ObjectOutputStream out) throws IOException{
        out.writeObject(user_map);
        out.writeObject(item_map);
    }
    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException{
        user_map = (HashMap<Integer,Record>) in.readObject();   
        item_map = (HashMap<Integer,Record>) in.readObject();
    }
}
