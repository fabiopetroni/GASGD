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

import java.util.LinkedList;

public class AverageStrategies {
    
    public static boolean updateProfile(Profile master_profile, LinkedList<Profile> slaves_profiles){
        //return addSlavesAverage(master_profile,slaves_profiles);
        return weightedAverage(master_profile,slaves_profiles);
    }
    
    /*
     * Whenever a master node has received all update vectors , 
     * it adds their average to the master copy and broadcasts the result.
     */
    private static boolean addSlavesAverage(Profile master_profile, LinkedList<Profile> slaves_profiles){
        if (slaves_profiles==null || slaves_profiles.isEmpty()){return false;}
        int features = master_profile.vector.length;
        int n = 0;
        double [] average_vector = new double[features];       
        for (int i = 0; i<features; i++){ average_vector[i]=0; }
        for (Profile p : slaves_profiles){
            for (int i = 0; i<features; i++){ average_vector[i]+=p.vector[i]; }
            p.resetCounter();
            n++;
        }
        for (int i = 0; i<features; i++){ average_vector[i]/=n; }        
        for (int i = 0; i<features; i++){ master_profile.vector[i] += average_vector[i];}
        master_profile.resetCounter();
        return true;
    }
    
    private static boolean weightedAverage(Profile master_profile, LinkedList<Profile> slaves_profiles){
        // AVG(n) = AVG(n-1) + ( (Wn / SUM_OF_WEIGHTS) * (Xn - AVG(n-1)) )
        if (slaves_profiles==null || slaves_profiles.isEmpty()){return false;}
        int features = master_profile.vector.length;
        double [] average_vector = new double[features];   
        int global_weight = 0; 
        for (int i = 0; i<features; i++){ average_vector[i]=0;}
        for (Profile p : slaves_profiles){
            int weight = p.getCounter();
            global_weight+=weight;
            for (int i = 0; i<features; i++){ 
                double old_average = average_vector[i];
                double factor = weight;
                factor /= global_weight;
                average_vector[i] = old_average + factor*(p.vector[i] - old_average);
            }
            p.resetCounter();
        }       
        int weight = master_profile.getCounter();
        global_weight+=weight;
        for (int i = 0; i<features; i++){ 
            double old_average = average_vector[i];
            double factor = weight;
            factor /= global_weight;
            average_vector[i] = old_average + factor*(master_profile.vector[i] - old_average);
            master_profile.vector[i] = (float) average_vector[i];
        }
        master_profile.resetCounter();
        return true;

    }
    
}
