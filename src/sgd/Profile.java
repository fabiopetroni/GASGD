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

import application.Globals.GENDER;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

public class Profile {

    protected GENDER gender;  
    protected float [] vector;
    protected int counter;
    private AtomicBoolean lock;
    
    protected Profile(int features, GENDER gender) {
        vector = new float[features];
        Random random = new Random();
        for (int f = 0; f < features; f++) { 
            // The starting points were chosen by taking i.i.d. samples from the Uniform( -0.5,0.5) distribution
            //nextDouble() Returns the next pseudorandom, uniformly distributed double value between 0.0 and 1.0 from this random number generator's sequence.
            vector[f] = (float) (random.nextDouble() - 0.5); 
        }
        this.gender = gender;
        lock = new AtomicBoolean(true);
    } 
    
    public Profile(GENDER gender) {
        this.gender = gender;
        lock = new AtomicBoolean(true);
    }
    
    protected Profile(Profile clone){
        int features = clone.vector.length;
        this.vector = new float[features];
        System.arraycopy(clone.vector, 0, this.vector, 0, features);
        this.gender = clone.gender;
        lock = new AtomicBoolean(true);
    }
    
    public synchronized boolean getLock(){
        return lock.compareAndSet(true, false);
    }
    
    public synchronized boolean releaseLock(){
        return lock.compareAndSet(false, true);
    }
    
    public synchronized GENDER getGender() {
        return gender;
    }

//    public synchronized double getVectorFeature(int k) {
//        return vector[k];
//    }

    protected synchronized int getCounter() {
        return counter;
    }

    protected synchronized void incrementCounter() {
        this.counter++;
    }
    
    protected synchronized void resetCounter() {
        this.counter = 0;
    }
}
