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
import core.Edge;
import application.Globals;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import partitioner.PartitionState;
import partitioner.Record;
import sgd.AverageStrategies;
import sgd.Profile;
import sgd.SGDState;
import sgd.Utility;

public class LocalSGD {
    
    private SGDState sgd_state;
    private int K;
    private double Lambda;
    private Machine machine;
    
    private TreeSet<Integer> slave_users_updated;
    private TreeSet<Integer> slave_items_updated;
    private TreeSet<Integer> master_users_updated;
    private TreeSet<Integer> master_items_updated;
    
    private HashMap<Integer,LinkedList<Profile>> buffer_slave_user_profile;
    private HashMap<Integer,LinkedList<Profile>> buffer_slave_item_profile;
    
    private boolean COMPUTE_LOSS;

    protected LocalSGD(int K, double l, Machine m, boolean CL) {
        this.K = K;
        this.Lambda = l;
        this.machine = m;
        this.COMPUTE_LOSS = CL;
        if (COMPUTE_LOSS){ 
            sgd_state = new SGDState(K); 
            buffer_slave_user_profile = new HashMap<Integer,LinkedList<Profile>>();
            buffer_slave_item_profile = new HashMap<Integer,LinkedList<Profile>>();
        }
        slave_users_updated = new TreeSet<Integer>();
        slave_items_updated = new TreeSet<Integer>();
        master_users_updated = new TreeSet<Integer>();
        master_items_updated = new TreeSet<Integer>();
    }
    
    //user's and item's vectors are initialized and synchronized between replicas and master
    protected void init(PartitionState partition_state){
        for (int user : machine.master_users){
            Profile p = sgd_state.initUserProfile(user);
            Record user_record = partition_state.getUserRecord(user);
            if (user_record.getMaster()!=machine.getId()){ System.out.println("LOGIC ERROR (master!=master): user_record.getMaster()["+user_record.getMaster()+"]!=machine.getId()["+machine.getId()+"]"); }  //TODO USELESS CHECK TO REMOVE
            Iterator<Byte> it = user_record.getSlave();
            while (it.hasNext()){
                int destination = it.next();
                sendInitMsg(user,p,destination);
            }
        }
        for (int item : machine.master_items){
            Profile p = sgd_state.initItemProfile(item);
            Record item_record = partition_state.getItemRecord(item);
            if (item_record.getMaster()!=machine.getId()){ System.out.println("LOGIC ERROR (master!=master): item_record.getMaster()["+item_record.getMaster()+"]!=machine.getId()["+machine.getId()+"]"); }  //TODO USELESS CHECK TO REMOVE
            Iterator<Byte> it = item_record.getSlave();
            while (it.hasNext()){
                int destination = it.next();
                sendInitMsg(item,p,destination);
            }
        }
    }
    
    //slot indicates the current percentage of update procedure completion. slot == (frequency - 1) is the last update for the epoch.
    //frequency indicates how many times during the update procedure the machine needs to comunicate with the others
    public void update(int slot, int frequency, double mu){  
        int n = machine.edges.size();
        double subSize = n ;
        subSize /= frequency;
        double iStart = slot * subSize;
        double iEnd = Math.min((slot + 1) * subSize, n);
        if (iStart>iEnd){ System.out.println("ERROR: iStart :"+iStart+", iEnd: "+iEnd+", n: "+n+", slot:"+slot+", frequency:"+frequency); System.exit(-1);}
        List<Edge> dataset= machine.edges.subList((int)iStart, (int)iEnd); // fromIndex, inclusive, and toIndex, exclusive
        
//        System.out.println("\n- machine: "+machine.getId()+"\n"+
//                            "slot: "+slot+"\n"+
//                            "frequency: "+frequency+"\n"+
//                            "n: "+n+"\n"+
//                            "subSize: "+subSize+"\n"+
//                            "iStart: "+iStart+"\n"+
//                            "iEnd: "+iEnd+"\n"+
//                            "list.size(): "+list.size()+"\n");
        
        
        
        int processors = 1;
        double aux = Runtime.getRuntime().availableProcessors();
        aux /= machine.net.GLOBALS.P;
        processors = (int) aux;
        
        if (processors>1 && COMPUTE_LOSS){ //UPDATE PHASE IN PARALLEL
            ExecutorService executor=Executors.newFixedThreadPool(processors);
            n = dataset.size();
            subSize = n / processors + 1;
            //System.out.println(dataset.size()); //DEBUG
            for (int t = 0; t < processors; t++) {
                iStart = t * subSize;
                iEnd = Math.min((t + 1) * subSize, n);
                if (iEnd>=iStart){
                    Runnable x = new LocalUpdateThread(dataset.subList((int)iStart, (int)iEnd),sgd_state, K, Lambda, mu, COMPUTE_LOSS, this);
                    executor.execute(x);
                }
            }
            try { 
                executor.shutdown();
                executor.awaitTermination(60, TimeUnit.DAYS);
            } catch (InterruptedException ex) {System.out.println("InterruptedException "+ex);ex.printStackTrace();}       
        }
        
        else{ //UPDATE PHASE NOT IN PARALLEL
            for (Edge e : dataset){
                int item = e.item;
                int user = e.user;
                notifyUserItemVectorsUpdated(user, item);
                if (COMPUTE_LOSS){
                    Profile user_profile = sgd_state.getUserProfile(user);
                    Profile item_profile = sgd_state.getItemProfile(item);
                    Utility.localUpdate(K, Lambda, mu, user_profile, item_profile , e.rating);
                }
            }
        }
    }
    
    protected synchronized void notifyUserItemVectorsUpdated(int user, int item){
        if (!machine.master_users.contains(user)){ slave_users_updated.add(user);}
        else{ master_users_updated.add(user);}
        if (!machine.master_items.contains(item)){ slave_items_updated.add(item);}
        else{ master_items_updated.add(item);}
    }
    
    public double computeLoss(){
        double loss = 0;
        for (Edge e : machine.edges){
            Profile user_profile = sgd_state.getUserProfile(e.user);
            Profile item_profile = sgd_state.getItemProfile(e.item);
            double localLoss = Utility.localLoss(K, Lambda, user_profile, item_profile, e.rating);
            loss += localLoss;
        }
        return loss;
    }
    
    
    private void sendInitMsg(int id, Profile p, int destination){
        Message msg = new Message(Globals.INIT_HEADER,p,id);
        machine.send(msg, destination);
    }
    
    protected synchronized void receiveInitMsg(Message msg, int sender){
        Profile p =  msg.payload;
        int id = msg.id;
        sgd_state.setProfile(id,p); 
    }
    
    private void sendMasterProfileMsg(int id, Profile p, int destination){
        Message msg = new Message(Globals.MASTER_PROFILE_HEADER,p,id);
        machine.send(msg, destination);
    }
    
    protected synchronized void receiveMasterProfileMsg(Message msg, int sender){
        if (COMPUTE_LOSS){
            Profile p = msg.payload;
            int id = msg.id;
            sgd_state.setProfile(id, p); 
        }
    }
    
    protected void sendSlaveProfiles(PartitionState partition_state){
        for (int user : slave_users_updated){
            Record user_record = partition_state.getUserRecord(user);
            if (user_record.getMaster()==machine.getId()){ System.out.println("LOGIC ERROR (slave=master): user_record.getMaster()["+user_record.getMaster()+"]==machine.getId()["+machine.getId()+"]"); }  //TODO USELESS CHECK TO REMOVE
            if (COMPUTE_LOSS){
                Profile user_profile = sgd_state.getUserProfile(user);
                sendSlaveProfile(user,user_profile,user_record.getMaster());
            }
            else{
                sendSlaveProfile(user,new Profile(Globals.GENDER.USER),user_record.getMaster());
            }
        }
        for (int item : slave_items_updated){
            Record item_record = partition_state.getItemRecord(item);
            if (item_record.getMaster()==machine.getId()){ System.out.println("LOGIC ERROR (slave=master): item_record.getMaster()["+item_record.getMaster()+"]==machine.getId()["+machine.getId()+"]"); }  //TODO USELESS CHECK TO REMOVE
            if (COMPUTE_LOSS){
                Profile item_profile = sgd_state.getItemProfile(item);
                sendSlaveProfile(item, item_profile,item_record.getMaster());
            }else{
                sendSlaveProfile(item, new Profile(Globals.GENDER.ITEM),item_record.getMaster());
            }
        }
        slave_users_updated.clear();
        slave_items_updated.clear();
    }
    
    protected synchronized void receiveSlaveProfileMsg(Message msg, int sender){
        Profile p = msg.payload;
        int id = msg.id;
        Globals.GENDER g = p.getGender();
        if (COMPUTE_LOSS){
            if (g==Globals.GENDER.ITEM){
                int item = id;
                if (!buffer_slave_item_profile.containsKey(item)){ buffer_slave_item_profile.put(item, new LinkedList<Profile>()); }
                buffer_slave_item_profile.get(item).add(p);
            }
            else if (g==Globals.GENDER.USER){
                int user = id;
                if (!buffer_slave_user_profile.containsKey(user)){ buffer_slave_user_profile.put(user, new LinkedList<Profile>()); }
                buffer_slave_user_profile.get(user).add(p);
            }
        }
        else {
            if (g==Globals.GENDER.ITEM){
                int item = id;
                master_items_updated.add(item);
            }
            else if (g==Globals.GENDER.USER){
                int user = id;
                master_users_updated.add(user);
            }
        }
    }
    
    private void sendSlaveProfile(int id, Profile p, int destination){
        Message msg = new Message(Globals.SLAVE_PROFILE_HEADER,p, id);
        machine.send(msg, destination);
    }
    
    protected void computeProfilesAverage(){
        if (COMPUTE_LOSS){
            for (int user : machine.master_users){
                Profile user_master_profile = sgd_state.getUserProfile(user);
                LinkedList<Profile> user_slaves_profiles = buffer_slave_user_profile.get(user);
                boolean updated = AverageStrategies.updateProfile(user_master_profile, user_slaves_profiles);
                if (updated){ master_users_updated.add(user); user_slaves_profiles.clear();}
            }
            for (int item : machine.master_items){
                Profile item_master_profile = sgd_state.getItemProfile(item);
                LinkedList<Profile> item_slaves_profiles = buffer_slave_item_profile.get(item);
                boolean updated = AverageStrategies.updateProfile(item_master_profile, item_slaves_profiles);
                if (updated){ master_items_updated.add(item); item_slaves_profiles.clear();}
            }
            buffer_slave_user_profile.clear();
            buffer_slave_item_profile.clear();
        }
    }
    
    //user's and item's vectors are updated and synchronized between replicas and master
    protected void sendUpdateProfiles(PartitionState partition_state){
        for (int user : master_users_updated){
            Profile user_master_profile;
            if (COMPUTE_LOSS){ user_master_profile = sgd_state.getUserProfile(user);}
            else{ user_master_profile = null; }
            Record user_record = partition_state.getUserRecord(user);
            if (user_record.getMaster()!=machine.getId()){ System.out.println("LOGIC ERROR (master!=master): user_record.getMaster()["+user_record.getMaster()+"]!=machine.getId()["+machine.getId()+"]"); }  //TODO USELESS CHECK TO REMOVE
            Iterator<Byte> it = user_record.getSlave();
            while (it.hasNext()){
                int destination = it.next();
                sendMasterProfileMsg(user, user_master_profile,destination);
            }
        }
        for (int item : master_items_updated){
            Profile item_master_profile;
            if (COMPUTE_LOSS){ item_master_profile = sgd_state.getItemProfile(item);}
            else{ item_master_profile = null; }
            Record item_record = partition_state.getItemRecord(item);
            if (item_record.getMaster()!=machine.getId()){ System.out.println("LOGIC ERROR (master!=master): item_record.getMaster()["+item_record.getMaster()+"]!=machine.getId()["+machine.getId()+"]"); }  //TODO USELESS CHECK TO REMOVE
            Iterator<Byte> it = item_record.getSlave();
            while (it.hasNext()){
                int destination = it.next();
                sendMasterProfileMsg(item, item_master_profile,destination);
            }
        }
        master_users_updated.clear();
        master_items_updated.clear();
    }
    
    protected void shuffleEdgesCollection(){
        Collections.shuffle(machine.edges);
    }
}
