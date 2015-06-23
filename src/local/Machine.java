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

package local;

import architecture.Message;
import architecture.Network;
import core.Edge;
import application.Globals;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.LinkedList;
import java.util.TreeSet;

public class Machine{
    
    private int id;
    protected Network net;
    
    protected LinkedList<Edge> edges;    
    protected LocalSGD f;    
    protected TreeSet<Integer> master_users;
    protected TreeSet<Integer> master_items;

    public Machine(int id, Network net) {
        this.id = id;
        this.net = net;
        this.edges =  new LinkedList<Edge>();
        this.master_users = new TreeSet<Integer>();
        this.master_items = new TreeSet<Integer>();
        f = new LocalSGD(this.net.GLOBALS.K,this.net.GLOBALS.Lambda,this,net.GLOBALS.COMPUTE_LOSS);
    }
    
    public int getId(){
        return id;
    }
    
    public synchronized void addMasterUser(int user){
        master_users.add(user);
    }
    
    public synchronized void addMasterItem(int item){
        master_items.add(item);
    }
    
    public synchronized void addEdge(Edge t){
        edges.add(t);
    }
    
    public synchronized int getEdgesNumber(){
        return edges.size();
    }
    
    public void send(Message msg, int destination){
        net.sendMsg(id, destination, msg);
    }
    
    public synchronized void receive(Message msg, int sender){
        if (msg.header.equals(Globals.INIT_HEADER)){
            f.receiveInitMsg(msg, sender);
        }
        if (msg.header.equals(Globals.SLAVE_PROFILE_HEADER)){
            f.receiveSlaveProfileMsg(msg, sender);
        }
        if (msg.header.equals(Globals.MASTER_PROFILE_HEADER)){
            f.receiveMasterProfileMsg(msg, sender);
        }
    }
    
    public void loadEdges() throws FileNotFoundException, IOException, ClassNotFoundException{
        FileInputStream fileIn = new FileInputStream(net.GLOBALS.MACHINE_STORED_EDGES_PATH+id);  
        ObjectInputStream in = new ObjectInputStream(fileIn);  
        edges = (LinkedList<Edge>) in.readObject(); 
        in.close();  
        fileIn.close();
    }
    
    public void storeEdges() throws FileNotFoundException, IOException{
        FileOutputStream fileOut = new FileOutputStream(net.GLOBALS.MACHINE_STORED_EDGES_PATH+id);  
        ObjectOutputStream outStream = new ObjectOutputStream(fileOut);  
        outStream.writeObject(edges);  
        outStream.close();  
        fileOut.close();  
    }
}
