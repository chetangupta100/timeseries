# timeseries

Crunching timeseries data

Project contains Main class Crunch.scala


Program to read file from input which contains timestamp and value. The program tries to group all the values within 60  secs and computes avg/ sum/ min/max of the given window. 

If the next epoch value is more than the start epoch of the  current window then the metrics of current window are flushed out to file and the metrics are reset.

Input : data/files/data.txt

Ouput : data/files/output.txt
 
