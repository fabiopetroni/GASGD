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

package sgd;

import application.Globals;
import java.util.HashMap;

public class SGDState {
    protected HashMap<Integer,Profile> user_map;
    protected HashMap<Integer,Profile> item_map;
    protected int K;

    public SGDState(int K) {
        this.K = K;
        user_map = new HashMap<Integer,Profile>();
        item_map = new HashMap<Integer,Profile>();
    }
    
    public synchronized Profile initUserProfile(int user){
        Profile p = new Profile(K, Globals.GENDER.USER);
        user_map.put(user, p);
        return p;
    }
    
    public synchronized Profile initItemProfile(int item){
        Profile p = new Profile(K, Globals.GENDER.ITEM);
        item_map.put(item, p);
        return p;
    }
    
    public synchronized void setProfile(int id, Profile p){
        Globals.GENDER g = p.getGender();
        if (g==Globals.GENDER.ITEM){
            item_map.put(id, new Profile(p));
        }
        else if (g==Globals.GENDER.USER){
            user_map.put(id, new Profile(p));
        }
    }
    
    public Profile getUserProfile(int user){
        return user_map.get(user);
    }
    
    public Profile getItemProfile(int item){
        return item_map.get(item);
    }
}
