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

public class UpdateThread implements Runnable{

    private Machine machine;
    private int slot; 
    private int frequency; 
    private double mu;

    public UpdateThread(Machine machine, int slot, int frequency, double mu) {
        this.machine = machine;
        this.slot = slot;
        this.frequency = frequency;
        this.mu = mu;
    }
    
    @Override
    public void run() {
        if (frequency>machine.getEdgesNumber()){ frequency = machine.getEdgesNumber();}
        machine.f.update(slot, frequency, mu);
    }
    
}
