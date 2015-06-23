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

package output;

import architecture.Message;
import architecture.Network;
import application.Globals;
import java.io.BufferedWriter;
import java.io.FileWriter;
import local.Machine;
import partitioner.PartitionState;

public class Statistics {
    
    private double NUM_INIT_MESSAGE;
    private double[] NUM_SLAVE_PROFILE_MESSAGE;
    private double[] NUM_MASTER_PROFILE_MESSAGE;  
    private int epoch;
    private Globals GLOBALS;
    
    private long START_TIME; 
    private long MEDIUM_TIME;
    private long END_TIME;

    public void setSTART_TIME(long START_TIME) {
        this.START_TIME = START_TIME;
    }

    public void setMEDIUM_TIME(long MEDIUM_TIME) {
        this.MEDIUM_TIME = MEDIUM_TIME;
    }

    public void setEND_TIME(long END_TIME) {
        this.END_TIME = END_TIME;
    }
    
    public Statistics(Globals G){
        this.GLOBALS = G;
        NUM_INIT_MESSAGE = 0;
        NUM_SLAVE_PROFILE_MESSAGE = new double[GLOBALS.iterations];
        NUM_MASTER_PROFILE_MESSAGE = new double[GLOBALS.iterations];
        for (int i = 0; i<GLOBALS.iterations; i++){
            NUM_SLAVE_PROFILE_MESSAGE[i]=0;
            NUM_MASTER_PROFILE_MESSAGE[i]=0;
        }
        epoch = 0;
    }
    
    public void reset(){
        NUM_INIT_MESSAGE = 0;
        for (int i = 0; i<GLOBALS.iterations; i++){
            NUM_SLAVE_PROFILE_MESSAGE[i]=0;
            NUM_MASTER_PROFILE_MESSAGE[i]=0;
        }
        epoch = 0;
    }
    
    public void incrementEpoch(){
        epoch++;
    }
    
    public synchronized void newMessage(int sender, int receiver, Message msg){
        if (msg.header.equals(Globals.INIT_HEADER)){
            NUM_INIT_MESSAGE++;
        }
        else if (msg.header.equals(Globals.SLAVE_PROFILE_HEADER)){
            NUM_SLAVE_PROFILE_MESSAGE[epoch]++;
        }
        else if (msg.header.equals(Globals.MASTER_PROFILE_HEADER)){
            NUM_MASTER_PROFILE_MESSAGE[epoch]++;
        }
        else{
            System.out.println("ERROR: UNKNOWN MESSAGE HEADER "+msg.header);
            System.exit(-1);
        }
    }
    
    double replication_factor;
    public void computeReplicationFactor(PartitionState state){
        int N_item = state.getItemNumber();
        int N_user = state.getUserNumber();
        int sum_of_replicas_items = state.getSumItemsCopies();
        int sum_of_replicas_users = state.getSumUsersCopies();
        double replication_factor_item = sum_of_replicas_items;
        replication_factor_item/=N_item;
        replication_factor_item = (double)Math.round(replication_factor_item*100)/100;
        double replication_factor_user = sum_of_replicas_users;
        replication_factor_user/=N_user;
        replication_factor_user = (double)Math.round(replication_factor_user*100)/100;
        replication_factor = sum_of_replicas_items+sum_of_replicas_users;
        replication_factor /= (N_user+N_item);
        replication_factor = (double)Math.round(replication_factor*100)/100;
//        System.out.println("- Replication Factor Statistics");
//        System.out.println("RF_items: "+replication_factor_item);
//        System.out.println("RF_users: "+replication_factor_user);
//        System.out.println("RF: "+replication_factor);
//        System.out.println();
//        System.out.println("N_item:"+N_item);
//        System.out.println("N_user:"+N_user);
//        System.out.println("sum_of_replicas_items:"+sum_of_replicas_items);
//        System.out.println("sum_of_replicas_users:"+sum_of_replicas_users);
    }
    public double getReplicationFactor(){
        return replication_factor;
    }
    
    //compute standard deviation of the load
    double std_dev_load;
    public void computeStdDevLoad(Network net){
        int num_machines = GLOBALS.P;
        int weight[] = new int[num_machines];
        double average_load = 0;
        for (int m = 0; m< num_machines; m++){            
            Machine machine = net.getMachine(m);
            weight[m] = machine.getEdgesNumber();
            average_load += weight[m];
        }
        average_load /= num_machines;
        double num = 0;
        for (int m = 0; m< num_machines; m++){
            num += Math.pow(weight[m] - average_load, 2);
        }
        num/=num_machines;
        std_dev_load = Math.sqrt(num);
        std_dev_load /= average_load; //Relative standard deviation
        /*      
        System.out.println("- Standard Deviation Statistics");
        System.out.println("average_load: "+average_load);
        System.out.println("num_machines: "+num_machines);
        for (int m = 0; m< num_machines; m++){
            System.out.println("load m"+m+": "+weight[m]);
        }
        System.out.println("num: "+num);
        System.out.println("std_dev_load: "+std_dev_load);*/
    }
    public double getStdDevLoad(){
        return std_dev_load;
    }
    
    @Override
    public String toString(){
        String s = "";
        s += "NUM_INIT_MESSAGE: "+NUM_INIT_MESSAGE+"\n";
        s += "NUM_SLAVE_PROFILE_MESSAGE: "+NUM_SLAVE_PROFILE_MESSAGE+"\n";
        s += "NUM_MASTER_PROFILE_MESSAGE: "+NUM_MASTER_PROFILE_MESSAGE+"\n";
        return s;
    }
    
    public void print(double [] losses){
        if (losses!=null){ printLosses(losses); }
        printCommunicationCost(GLOBALS.iterations);
        printInfo();
    }
    
    private void printLosses(double [] losses){
        try {
            FileWriter fstream = new FileWriter(GLOBALS.OUTPUT_LOSS_FILE);
            BufferedWriter out = new BufferedWriter(fstream);
            for (int i = 0; i<losses.length; i++){
                int epoch = i+1;
                out.write(epoch+" "+losses[i]+"\n");
            }
            out.close();
            fstream.close();
        } catch (Exception ex) {
            System.out.println("ERRORE "+ex);
            ex.printStackTrace();
            System.exit(-1);
        } 
    }
    
    private void printInfo(){
        try {
            FileWriter fstream = new FileWriter(GLOBALS.OUTPUT_INFO_FILE);
            BufferedWriter out = new BufferedWriter(fstream);
            out.write(GLOBALS.toString());
            out.close();
            fstream.close();
        } catch (Exception ex) {
            System.out.println("ERRORE "+ex);
            ex.printStackTrace();
            System.exit(-1);
        } 
    }
    
    private void printCommunicationCost(int epoch){
        double TOTAL_NUM_MSG = NUM_INIT_MESSAGE;
        double TOT_SLAVE_MSG = 0;
        double avg_slave_msg = 0;
        for (int i = 0; i<NUM_SLAVE_PROFILE_MESSAGE.length; i++){
            double slave_msg_epoch = NUM_SLAVE_PROFILE_MESSAGE[i];
//            System.out.println(i+" "+slave_msg_epoch);
            TOT_SLAVE_MSG += slave_msg_epoch;
            double sum = slave_msg_epoch - avg_slave_msg;
            sum /= (i+1);
            avg_slave_msg += sum;
        }
        
        double TOT_MASTER_MSG = 0;
        double avg_master_msg = 0;
        for (int i = 0; i<NUM_MASTER_PROFILE_MESSAGE.length; i++){
            double master_msg_epoch = NUM_MASTER_PROFILE_MESSAGE[i];
//            System.out.println(i+" "+master_msg_epoch);
            TOT_MASTER_MSG += master_msg_epoch;
            double sum  = master_msg_epoch - avg_master_msg;
            sum /= (i+1);
            avg_master_msg += sum;
        }
        double NUM_MSG_PER_EPOCH = avg_master_msg + avg_slave_msg;
        TOTAL_NUM_MSG += TOT_SLAVE_MSG + TOT_MASTER_MSG;
                
        try {
            FileWriter fstream = new FileWriter(GLOBALS.OUTPUT_COMMUNICATION_COST_FILE);
            BufferedWriter out = new BufferedWriter(fstream);
            out.write("EPOCH: "+epoch+"\n");
            out.write("TOT MSG: "+TOTAL_NUM_MSG+"\n");
            out.write("TOT INIT MSG: "+NUM_INIT_MESSAGE+"\n");
            out.write("TOT SLAVE MSG: "+TOT_SLAVE_MSG+"\n");
            out.write("TOT MASTER MSG: "+TOT_MASTER_MSG+"\n");
            out.write("MSG PER EPOCH: "+NUM_MSG_PER_EPOCH+"\n");
            out.write("SLAVE MSG PER EPOCH: "+avg_slave_msg+"\n");
            out.write("MASTER MSG PER EPOCH: "+avg_master_msg+"\n");  
            out.write("RF: "+replication_factor+"\n");  
            long PARTITIONING_TIME = MEDIUM_TIME - START_TIME;
            long ALGORITHM_TIME = END_TIME - MEDIUM_TIME;
            out.write("PARTITIONING TIME: "+PARTITIONING_TIME+"\n");  
            out.write("ALGORITHM TIME: "+ALGORITHM_TIME+"\n");  
            double std_dev = Math.floor(std_dev_load * 100)/100;
            out.write("STD DEV LOAD: "+std_dev+" ("+std_dev_load+")\n"); 
            System.out.print("EPOCH: "+epoch+"\n");
            System.out.print("TOT MSG: "+TOTAL_NUM_MSG+"\n");
            System.out.print("TOT INIT MSG: "+NUM_INIT_MESSAGE+"\n");
            System.out.print("TOT SLAVE MSG: "+TOT_SLAVE_MSG+"\n");
            System.out.print("TOT MASTER MSG: "+TOT_MASTER_MSG+"\n");
            System.out.print("MSG PER EPOCH: "+NUM_MSG_PER_EPOCH+"\n");
            System.out.print("SLAVE MSG PER EPOCH: "+avg_slave_msg+"\n");
            System.out.print("MASTER MSG PER EPOCH: "+avg_master_msg+"\n");
            System.out.print("STD DEV LOAD: "+std_dev+" ("+std_dev_load+")\n");
            System.out.print("PARTITIONING TIME: "+PARTITIONING_TIME+"\n");  
           System.out.print("ALGORITHM TIME: "+ALGORITHM_TIME+"\n");  
            out.close();
            fstream.close();
        } catch (Exception ex) {
            System.out.println("ERRORE "+ex);
            ex.printStackTrace();
            System.exit(-1);
        }   
    }
}
