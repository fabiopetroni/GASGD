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

package input;

import core.Edge;
import application.Globals;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.LinkedList;
import java.util.Random;
import java.util.TreeSet;

public class YahooInput {
    private Globals GLOBALS;
    private LinkedList<Edge> dataset;
    
    public YahooInput(Globals G){
        this.GLOBALS = G;
        this.dataset = new LinkedList<Edge>();    
        loadDatasetInMainMemory();
    }

    private void loadDatasetInMainMemory(){
        if (GLOBALS.LOAD_FROM_DISK){
            try{
                //FAST ACCESS TO OBJECT IN STORAGE
                FileInputStream fileIn = new FileInputStream(GLOBALS.DATASET_OBJECT_PATH);  
                ObjectInputStream in = new ObjectInputStream(fileIn);  
                dataset = (LinkedList<Edge>) in.readObject();  
                GLOBALS.setNUM_ITEMS(in.readInt());
                GLOBALS.setNUM_USERS(in.readInt());
                in.close();  
                fileIn.close();
            }catch(Exception e1){
                System.out.println("Dataset deserialization Exception "+e1);
                try{
                    readDatasetFromFile();
                    //STORE THE OBJECT ON STORAGE FOR FAST ACCESS
                    FileOutputStream fileOut = new FileOutputStream(GLOBALS.DATASET_OBJECT_PATH);  
                    ObjectOutputStream outStream = new ObjectOutputStream(fileOut);  
                    outStream.writeObject(dataset);  
                    outStream.writeInt(GLOBALS.NUM_ITEMS);
                    outStream.writeInt(GLOBALS.NUM_USERS);
                    outStream.close();  
                    fileOut.close();  
                }catch(Exception e2){
                    System.out.println("Partition state serialization Exception "+e2);
                    e2.printStackTrace();
                    System.exit(-1);
                }
            }
        }
        else{ readDatasetFromFile(); }
    }
    
    
//    private TreeSet<Integer> users;
//    private TreeSet<Integer> items;
    private void readDatasetFromFile(){
//        users = new TreeSet<Integer>();
//        items = new TreeSet<Integer>();
        try {
            FileInputStream fis = new FileInputStream(new File(GLOBALS.INPUT_FILE_NAME));
            InputStreamReader isr = new InputStreamReader(fis);
            BufferedReader in = new BufferedReader(isr);
            
            String strLine;
            
//            int n = 0;
//            Random random = new Random();
////            double percentage_of_ratings_taken = 0.6;

            //PRINT PERCENTAGE COMPLETAMENTO
            int line_number = 0;
            int num_righe_totale = 253801265;
            int grane = 100;
            double z = num_righe_totale;
            z/= grane;
            int step = (int) z;

            int user_id = 0;
            int item_id = 0;
            byte rating = 0;
            
            while ((strLine = in.readLine()) != null)   {
                line_number++;

                //PRINT PERCENTAGE COMPLETAMENTO
                if (line_number%(step)==0){
                    double x = line_number;
                    x /= num_righe_totale;
                    x *= 100;
                    int percentage = (int) Math.round(x);
                    System.out.println(percentage+"%");
                }

                String [] split = strLine.split("\t");

                // day_number|number_of_ratings
                if (split.length==1){
                        String user = strLine.substring(0,strLine.indexOf("|"));
                        //DAY_NUMBER : split[0]
                        //System.out.println(day_number);
                        user_id = Integer.parseInt(user);
    //		    System.out.println(day);
                }

                //rating
                else{
//                    double x = random.nextDouble();
//                    //System.out.println(x);
//                    if(x <= percentage_of_ratings_taken){
                        item_id = Integer.parseInt(split[0]);
                        rating = Byte.parseByte(split[1]);	
//                        users.add(user_id);
//                        items.add(item_id);
                        Edge t = new Edge(user_id,item_id,rating);
                        dataset.add(t);
//                        n++;
//                    }
                }
//                if (line_number==1000){break;}ß
            }
//            GLOBALS.setNUM_ITEMS(items.size());
//            GLOBALS.setNUM_USERS(users.size());
//            System.out.println("ITEMS: "+items.size());
//            System.out.println("USERS: "+users.size());
//            System.out.println("TRAINING POINTS: "+n);
//            users.clear();
//            items.clear();           
            in.close();
        } catch (IOException ex) {
            System.out.println("\nError: loadDatasetInMainMemory.\n\n");
            ex.printStackTrace();
            System.exit(-1);
        }   
    }
    
    public LinkedList<Edge> getDataset(){
        return dataset;
    }
}