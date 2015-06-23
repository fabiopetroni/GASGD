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
import local.Machine;

public class Hashing implements PartitionStrategy{

    double seed;
    public Hashing() {
        seed = Math.random();
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
        
        int machine_id = Math.abs((int) ( (int) user*item*seed) % P);  
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
