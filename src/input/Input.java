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
import java.util.TreeSet;

public class Input {
    private Globals GLOBALS;
    private LinkedList<Edge> dataset;
    
    public Input(Globals G){
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
    
    
    private TreeSet<Integer> users;
    private TreeSet<Integer> items;
    private void readDatasetFromFile(){
        users = new TreeSet<Integer>();
        items = new TreeSet<Integer>();
        try {
            FileInputStream fis = new FileInputStream(new File(GLOBALS.INPUT_FILE_NAME));
            InputStreamReader isr = new InputStreamReader(fis);
            BufferedReader in = new BufferedReader(isr);
            String line;
            while((line = in.readLine())!=null){
                String values[] = line.split(GLOBALS.INPUT_FILE_SEPARATOR);
                int user = Integer.parseInt(values[0]);
                int item = Integer.parseInt(values[1]);
                users.add(user);
                items.add(item);
                double aux = Double.parseDouble(values[2]);
                byte rating = (byte) aux;
                Edge t = new Edge(user,item,rating);
                dataset.add(t);
            }
            GLOBALS.setNUM_ITEMS(items.size());
            GLOBALS.setNUM_USERS(users.size());
            users.clear();
            items.clear();           
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