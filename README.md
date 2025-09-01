# Big Data project
Elisa Albertini and Eleonora Bertoni

## Chosen dataset
[Link to dataset](https://www.kaggle.com/datasets/sayedmahmoud/amazanreviewscor5)

## Project structure
- dataset folder: contains dataset samples for notebooks
- src\main\python\notebook folder: contains notebook of the two jobs and of their optimized versions
- src\main\scala\projectJobs folder: contains the scala jobs and their optimized versions
- history folder: contains the Spark history of all the four scala jobs

## Execution
- cluster: 
    - master node: one m4.large instance  
    - core nodes: four m4.xlarge instance 
- executor:
    - executor memory: 10G
    - executor number: 4
    - executor core: 3
- deploy: cluster mode
- Spark configuration: disabled dynamic allocation

