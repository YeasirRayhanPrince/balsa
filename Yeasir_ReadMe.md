`plans_lib.py`  
- The basis of an AST, i.e., a node

`envs.py`
- Workload: Loads queries and converts them to an AST 

`Sim.py`
- You collect simulation data
- You featurize the simulation data 
  - `_FeaturizeTrainingData` Check if it is there is (`_LoadFeaturizedData`)
  - No, check if simulation data is there `CollectSimulationData` 
  - Yes, load from there `_LoadSimulationData`
    - Data Format: `SubplanGoalCost`  
    - This one has the form state, goal, cost


