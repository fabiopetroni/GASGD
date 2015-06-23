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

public class Utility{
    
    public static double localLoss(int K, double Lambda, Profile user_profile, Profile item_profile , double rating){
        double prediction = dotproduct(K,user_profile, item_profile);
        double err = rating - prediction;
        double local_loss = Math.pow(err, 2);
        local_loss /= 2;

        double regularization = Lambda;
        regularization /=2;
        regularization *= (FrobeniusNorm(K,user_profile) + FrobeniusNorm(K,item_profile));

        local_loss += regularization;
        return local_loss;
    }
    
    public static void localUpdate(int K, double Lambda, double mu, Profile user_profile, Profile item_profile , double rating){
        user_profile.incrementCounter();
        item_profile.incrementCounter();
        double prediction = dotproduct(K,user_profile, item_profile);
        double err = rating - prediction;
        for (int k = 0; k < K; k++) {
            double userFeature = user_profile.vector[k];
            double itemFeature = item_profile.vector[k];
            double user_std_increment = err * itemFeature;
            double item_std_increment = err * userFeature;
            double user_regularization_increment = Lambda * userFeature;
            double item_regularization_increment = Lambda * itemFeature;
            double user_increment = mu * (user_std_increment - user_regularization_increment);
            double item_increment = mu * (item_std_increment - item_regularization_increment);  
            user_profile.vector[k] += user_increment;
            item_profile.vector[k] += item_increment;
            
            //CHECK IF THE USER VECTOR OVERFLOWS
            if (Double.isNaN(user_profile.vector[k]) || user_profile.vector[k]==Double.POSITIVE_INFINITY || user_profile.vector[k]==Double.NEGATIVE_INFINITY){
                long id = Thread.currentThread().getId();
                System.out.println("T"+id+": ERROR: user_profile.vector["+k+"]="+user_profile.vector[k]);
                System.out.println("T"+id+": old_user_profile.vector["+k+"]="+userFeature);
                System.out.println("T"+id+": err="+err);
                System.out.println("T"+id+": increment="+user_increment);
                System.out.println("T"+id+": user_std_increment="+user_std_increment);
                System.out.println("T"+id+": user_regularization_increment="+user_regularization_increment);
                System.out.println("T"+id+": itemFeature="+itemFeature);
                System.out.println();
                System.exit(-1);
            }
            
            //CHECK IF THE ITEM VECTOR OVERFLOWS
            if (Double.isNaN(item_profile.vector[k]) || item_profile.vector[k]==Double.POSITIVE_INFINITY || item_profile.vector[k]==Double.NEGATIVE_INFINITY){
                long id = Thread.currentThread().getId();
                System.out.println("T"+id+": ERROR: item_profile.vector["+k+"] Nan");
                System.out.println("T"+id+": old_item_profile.vector["+k+"]="+itemFeature);
                System.out.println("T"+id+": err="+err);
                System.out.println("T"+id+": increment="+item_increment);
                System.out.println("T"+id+": item_std_increment="+item_std_increment);
                System.out.println("T"+id+": item_regularization_increment="+item_regularization_increment);
                System.out.println("T"+id+": userFeature="+userFeature);
                System.out.println();
                System.exit(-1);
            }
        }  
    }
    
    private static double dotproduct(int K, Profile user_profile, Profile item_profile) {
        double sum = 0;
        for (int k = 0; k < K; k++) {
            sum += user_profile.vector[k] * item_profile.vector[k];
        }
        return sum;
    }  
    
    private static double FrobeniusNorm(int K, Profile profile){
        double sum = 0;
        for (int k = 0; k < K; k++) {
            sum += Math.pow(profile.vector[k],2);
        }
        return sum;
    }
}
