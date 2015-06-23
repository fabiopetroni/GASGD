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

import architecture.Network;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import local.ComputeLossThread;
import local.ComputeProfilesAverageThread;
import local.InitSGDStateThread;
import local.Machine;
import local.SendSlaveProfilesThread;
import local.SendUpdatedProfilesThread;
import local.ShuffleEdgesCollectionThread;
import local.UpdateThread;
import output.Statistics;
import partitioner.PartitionState;

public class DSGD {
    
    private Network net;
    private PartitionState partiton_state;
    
    private int K;
    private double Lambda;
    private double mu;
    private int iteration;
    
    private int frequency;
    private int num_machines;
    private int processors;
    
    private Statistics stat;

    public DSGD(Network net, PartitionState ps, Statistics s) {
        this.net = net;
        this.partiton_state = ps;
        K = net.GLOBALS.K;
        Lambda = net.GLOBALS.Lambda;
        mu = net.GLOBALS.init_mu;
        frequency = net.GLOBALS.Frequency;
        num_machines = net.GLOBALS.P;
        iteration = net.GLOBALS.iterations;
        this.stat = s;
        processors = Math.min(Runtime.getRuntime().availableProcessors(),net.GLOBALS.P);
    }
    
    private void initiazlie(){
        ExecutorService executor=Executors.newFixedThreadPool(processors);
        for (int m = 0; m<num_machines; m++){
            Machine machine = net.getMachine(m);
            Runnable x = new InitSGDStateThread(machine, partiton_state);
            executor.execute(x);
        }
        try { 
            executor.shutdown();
            executor.awaitTermination(60, TimeUnit.DAYS);
        } catch (InterruptedException ex) {System.out.println("InterruptedException "+ex);ex.printStackTrace();}
    }  
    
    public double[] perform(){
        
        if (net.GLOBALS.COMPUTE_LOSS){  //COMPLETE SGD, WITH PROFILE UPDATES
            double [] losses = new double[iteration]; 
            int epoch = 0;
            double old_loss = Double.POSITIVE_INFINITY;
            initiazlie();
            System.out.println("\tepoch\tloss");
            for (int i = 0; i<iteration; i++){
                shuffleEdgesCollections();
                runEpoch();
                double loss = computeLoss();
                losses[epoch] = loss;
                epoch++;
                System.out.println("\t"+epoch+"\t"+loss);
                BoldDriver(old_loss,loss);
                old_loss = loss;
                stat.incrementEpoch();
            }
            return losses;
        }
        else{ //LIGTH SGD, WITHOUT PROFILE UPDATES
            int epoch = 0;
            for (int i = 0; i<iteration; i++){
                shuffleEdgesCollections();
                runEpoch();
                epoch++;
                stat.incrementEpoch();
            }
        }
        return null;
    }
    
    private void shuffleEdgesCollections(){
        ExecutorService executor=Executors.newFixedThreadPool(processors);
        for (int m = 0; m<num_machines; m++){
            Machine machine = net.getMachine(m);
            Runnable x = new ShuffleEdgesCollectionThread(machine);
            executor.execute(x);
        }
        try { 
            executor.shutdown();
            executor.awaitTermination(60, TimeUnit.DAYS);
        } catch (InterruptedException ex) {System.out.println("InterruptedException "+ex);ex.printStackTrace();}
    }
    
    private void runEpoch(){
        ExecutorService executor;
        
        for (int slot = 0; slot<frequency; slot++){
            executor=Executors.newFixedThreadPool(processors);
            
            //1 - UPTATE LOCAL PROFILES FOR THE CURRENT SLOT
            for (int m = 0; m<num_machines; m++){
                Machine machine = net.getMachine(m);
                if (slot<=machine.getEdgesNumber()){
                    Runnable x = new UpdateThread(machine, slot, frequency, mu);
                    executor.execute(x);
                }
            }
            try { 
                executor.shutdown();
                executor.awaitTermination(60, TimeUnit.DAYS);
            } catch (InterruptedException ex) {System.out.println("InterruptedException "+ex);ex.printStackTrace();}
        
            //2 - COMMUNICATION PHASE FOR THE CURRENT SLOT
            //a) replicas send profiles to masters
            executor=Executors.newFixedThreadPool(processors);
            for (int m = 0; m<num_machines; m++){
                Machine machine = net.getMachine(m);
                Runnable x = new SendSlaveProfilesThread(machine, partiton_state);
                executor.execute(x);
            }
            try { 
                executor.shutdown();
                executor.awaitTermination(60, TimeUnit.DAYS);
            } catch (InterruptedException ex) {System.out.println("InterruptedException "+ex);ex.printStackTrace();}
        
            //b) masters compute average
            executor=Executors.newFixedThreadPool(processors);
            for (int m = 0; m<num_machines; m++){
                Machine machine = net.getMachine(m);
                Runnable x = new ComputeProfilesAverageThread(machine);
                executor.execute(x);
            }
            try { 
                executor.shutdown();
                executor.awaitTermination(60, TimeUnit.DAYS);
            } catch (InterruptedException ex) {System.out.println("InterruptedException "+ex);ex.printStackTrace();}
            
            //c) masters send average to the replicas
            executor=Executors.newFixedThreadPool(processors);
            for (int m = 0; m<num_machines; m++){
                Machine machine = net.getMachine(m);
                Runnable x = new SendUpdatedProfilesThread(machine, partiton_state);
                executor.execute(x);
            }
            try { 
                executor.shutdown();
                executor.awaitTermination(60, TimeUnit.DAYS);
            } catch (InterruptedException ex) {System.out.println("InterruptedException "+ex);ex.printStackTrace();}
        }
        
    }
    
    private double computeLoss(){
        double loss = 0;
        ComputeLossThread[] theads = new ComputeLossThread[num_machines];
        ExecutorService executor=Executors.newFixedThreadPool(processors);
        for (int m = 0; m<num_machines; m++){
            Machine machine = net.getMachine(m);
            theads[m] = new ComputeLossThread(machine);
            executor.execute(theads[m]);
        }
        try { 
            executor.shutdown();
            executor.awaitTermination(60, TimeUnit.DAYS);
        } catch (InterruptedException ex) {System.out.println("InterruptedException "+ex);ex.printStackTrace();}
        for (int m = 0; m<num_machines; m++){ 
            double machine_loss = theads[m].getResult(); 
            //System.out.println(m+" "+machine_loss); //DEBUG
            loss += machine_loss;
        }
        return loss;
    }
    
    public void BoldDriver(double old_loss, double new_loss){
        if (old_loss>new_loss){ mu *= 1.05; }
        else{ mu *= 0.5; }
    }
}