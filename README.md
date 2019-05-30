# EDGAR_Code
Code for EDGAR project 
File_Pairs runs on CPU and pairs 10Q and 10K files in same calendar quarters across years  
Sim_Calc runs as an array job on GPU and takes as input File_Pairs. It outputs Cosine and Jaccard similarity scores for all pairs in the input file
