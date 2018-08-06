# Twitter Event Detection Reproducibilty Toolkit for ECIR2019

The toolkit consists of three modules -- Twistor (artifical Twitter stream simulator), the evaluation module, and the event detection modules.

Here are the steps to setup the project in Eclipse:

1. Create an eclipse project
2. Import the source code cloned from the repository
3. Add the libraries from the *lib* folder to your Java Build Path
4. Have a look at the package *main* for starting the different modules.

## Twistor
Twistor is forked from the [Twistor Repository](https://github.com/HarryEuro/Twistor), you can find a introduction and more details about it there. However, we wanted a repository with a self-contained reproducibilty toolkit and therefore added the source code to this repository.

## Event Detection Modules
The event detection modules are based on the data stream management system [Niagarino Repository](https://github.com/DBIS-UniKN/niagarino). We included the Niagarino project as JAR for executing the two event detection modules *Shifty* and *Log-Likelihood Ratio* and the two baseline methods *TopN* and *Random Events*.

## Evaluation Module
The evaluation module compares the resulting events from the event detection modules with the generated events description file from the *Twistor* module.
