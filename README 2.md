# University of Waterloo: CS456 Fall 2019 Assignment 3
### About:
This zip file contains the scripts for executing the code. In the code folder there's all the code assignment 3.
Updated info for the second submission will appear in **_this format_** in order to make easier to read changes

### Compilation:
Since the programs are written in python, there is no need of makefile for compiling and executing the code.

### Execution:
**_To execute use the provided scripts_** tracker.sh **_and_** peer.sh. After giving the scripts permission
to execute simply type **./tracker.sh** for executing the tracker and
**./peer.sh <addressTracker> <portTracker> <minTimeinSeconds>** for executing the peer

### Additional notes
The code for the multiple threads and its pools was based from different online sources. As far as I've been able to test it works.\
Connecting any number of peers either from the same machine or from other machines will work.

### Changes in this last version (Due date Sunday 15 by 6pm)
Functional scripts where added (I wasn't able to have functioning scripts for my previous submission)
I know they are extremely simple but I was running out of time the delivery date and I had been
using an IDE for coding it. Nothing with respect to the code functionality was modified in any way.
There was a small change in tracker as I printed the word ADQUIRED instead of ACQUIRED. That was the only modification.
I've tested on the three machines we have access to and it should work with peers connected to different machines and all of them having multiple files + files over 100MB
