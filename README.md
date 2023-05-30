# Cross system Jobanalyzer
Easy to use resource usage report 

## Use case
* User X runs a analysis using a Python library called Pytorch. X expects the code to use GPUs. X wants to check this for the last 10 analysis that had finished 
* User Y submits a HPC job expecting to use 16 cores and 8Gb memory per CPU. Admins complain that Y is wasting resources. Y want to check how much resources the job just finished used 
* User Z wants to profile his matrix muluplication program in C++ and Z wants to know if it scales.
* Show the current load of a shared server and get historical usage statics

## What is expected 
* There are good profilers already present, but IFAIK, you need to commision the profile when the job starts. Is there a generic way to get simple statistics for a job. It might be that there is no way around this.
* Is there a less invasive way, may be with software already present to monitor all jobs.
* Current code that creates the load dashboard on ML nodes  https://github.uio.no/ML/dashboard 


