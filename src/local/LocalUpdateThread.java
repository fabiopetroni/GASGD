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

import core.Edge;
import java.util.LinkedList;
import java.util.List;
import sgd.Profile;
import sgd.SGDState;
import sgd.Utility;

public class LocalUpdateThread implements Runnable{

    private List<Edge> list;
    private SGDState sgd_state;
    private int K; //number of features
    private double Lambda;
    private double mu;
    private boolean COMPUTE_LOSS;
    private LocalSGD f;    

    public LocalUpdateThread(List<Edge> list, SGDState state, int K, double Lambda, double mu, boolean COMPUTE_L, LocalSGD f) {
        this.list = list;
        this.sgd_state = state;
        this.K = K;
        this.Lambda = Lambda;
        this.mu = mu;
        this.COMPUTE_LOSS = COMPUTE_L;
        this.f = f;
    }
    
    @Override
    public void run() {
        int sleep = 2;
        LinkedList<Edge> pending = new LinkedList<Edge>();
        
        for (Edge e : list){
            int user = e.user;
            int item = e.item;
            double rating = e.rating;
            
            Profile user_profile = sgd_state.getUserProfile(user);
            Profile item_profile = sgd_state.getItemProfile(item);
            if (user_profile.getLock()){
                if (item_profile.getLock()){
                    //WE HAVE THE LOCKS
                    this.update(user, item, user_profile,item_profile,rating);
                    item_profile.releaseLock();
                } else{ pending.add(e); }
                user_profile.releaseLock();
            } else{ pending.add(e); }
        }
        
        //OPTIMISTIC LIST OF PENDING TRANSACTIONS
        for (Edge e : pending){
            int user = e.user;
            int item = e.item;
            double rating = e.rating;
            Profile user_profile = sgd_state.getUserProfile(user);
            Profile item_profile = sgd_state.getItemProfile(item);
            sleep = 2; while (!user_profile.getLock()){ try{ Thread.sleep(sleep); }catch(Exception ex){} sleep = (int) Math.pow(sleep, 2); }
            sleep = 2; while (!item_profile.getLock()){ try{ Thread.sleep(sleep); }catch(Exception ex){} sleep = (int) Math.pow(sleep, 2); }
            this.update(user, item, user_profile,item_profile,rating);
            item_profile.releaseLock();
            user_profile.releaseLock();
        }
    }
    
    private void update(int user, int item, Profile user_profile, Profile item_profile, double rating){
        f.notifyUserItemVectorsUpdated(user, item);
        if (COMPUTE_LOSS){
            Utility.localUpdate(K, Lambda, mu, user_profile, item_profile , rating);
        }
    }
}
