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

import java.io.Serializable;
import java.util.Iterator;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;

public class Record implements Serializable{
    private byte master_machine;
    private TreeSet<Byte> slave_machines;  //BitSet
    private AtomicBoolean lock;
    private int degree;
    
    public Record() {
        slave_machines = new TreeSet<Byte>();
        master_machine = -1;
        lock = new AtomicBoolean(true);
        degree = 0;
    }
    
    public synchronized void increeseDegree(){
        degree++;
    }
    
    public synchronized int getDegree(){
        return degree;
    }
    
    public synchronized void setMaster(int machine){
        master_machine = (byte) machine;
    }
    
    public synchronized boolean hasMaster(){
        return master_machine!=-1;
    }

    public synchronized int getMaster() {
        return master_machine;
    }
    
    public synchronized Iterator<Byte> getSlave(){
        return slave_machines.iterator();
    }
    
    public synchronized void addSlave(int m){
        slave_machines.add( (byte) m);
    }
    
    public synchronized boolean getLock(){
        return lock.compareAndSet(true, false);
    }
    
    public synchronized boolean releaseLock(){
        return lock.compareAndSet(false, true);
    }
    
    public synchronized boolean contains(int m){
        if (master_machine==m){ return true; }
        if (slave_machines.contains((byte)m)){ return true; }
        return false;
    }
    
    public static TreeSet<Byte> intersection(Record x, Record y){
        TreeSet<Byte> result = new TreeSet<Byte>();
        //check the master
        byte m = x.master_machine;  
        if (y.master_machine==m){ result.add(m); }
        if (y.slave_machines.contains(m)){ result.add(m); }
        
        //check the slaves
        for (byte s : x.slave_machines){
            if (y.master_machine==s){ result.add(s); }
            if (y.slave_machines.contains(s)){ result.add(s); }
        }
        return result;
    }
    
    public int getCopies(){
        if (hasMaster()){ return slave_machines.size() + 1 ; }
        else{ return 0; }
    }
}