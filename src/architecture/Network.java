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

package architecture;

import application.Globals;
import local.Machine;
import output.Statistics;

public class Network {
    public Globals GLOBALS;
    private Machine[] machines;
    private Statistics stat;

    public Network(Globals GLOBALS, Statistics s) {
        this.GLOBALS = GLOBALS;
        machines = new Machine[GLOBALS.P];
        for (int i = 0; i< machines.length; i++){
            machines[i] = new Machine(i,this);
        }
        this.stat = s;
    }
    
    public Machine getMachine(int x){
        return machines[x];
    }
    
    public void sendMsg(int sender, int receiver, Message msg){
        stat.newMessage(sender, receiver, msg);
        machines[receiver].receive(msg, sender);
    }
}
