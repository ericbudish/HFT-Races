# HFT-Races

This repository contains code for researchers, regulators or practitioners who wish to use financial-exchange message data to quantify latency arbitrage and study other aspects of speed-sensitive trading, following Matteo Aquilina, Eric Budish and Peter O’Neill, [“Quantifying the High-Frequency Trading ‘Arms Race’”](https://faculty.chicagobooth.edu/eric.budish/research/Quantifying-HFT-Races.pdf), Quarterly Journal of Economics, 2021 (hereafter, “ABO”). The Python code processes the user's message data, detects trading races, and outputs a race-level statistical dataset along with complementary trading data. The R code produces race summary statistics, tables and figures analogous to all reported results in ABO. We also provide a small artificial data set that can be used to understand the data structure and to test one's configuration.

This code should be used in conjunction with the detailed documentation linked below. 

## About

Version 1.0 (September 2021). Please visit [https://github.com/ericbudish/HFT-Races](https://github.com/ericbudish/HFT-Races) to check for updates. 

## Documentation

Please refer to [ABO Code and Data Appendix](Code_and_Data_Appendix.pdf) for detailed documentation and instructions.

## Quick Start

1. Download and unzip this repository. Obtain the exchange message data and pre-process the data following Sections 2 and 3 of the [ABO Code and Data Appendix](Code_and_Data_Appendix.pdf). You may want to use the artificial dataset provided [here](ArtificialTestData/2000-01-01) to test your configuration first. 
2. Decide on the set of race detection parameters as instructed in Section 4 of the appendix. You can use the sample race detection parameters provided [here](ArtificialTestData/Sample_Input_Race_Parameters.csv) for testing.
3. Make sure Python 3 is installed on your MacOS/Linux system. For Windows users, we recommend using the Windows Subsystem for Linux. Please refer to Section 5 of the appendix for a complete guide on computational environment setup. Install Pandas and Numpy using the following command if you have not.
```
pip install -r requirements.txt # Install the required Python packages
``` 
4. Specify the parameters and file paths in [`process_msg_data_main.py`](PythonCode/process_msg_data_main.py) and run the script in the console using the command below. To test your configuration, set the file paths accordingly to use the artificial test data provided [here](ArtificialTestData). Please refer to Section 6 of the ABO Code and Data Appendix.  
```
nohup python3 path/to/code/process_msg_data_main.py > path/to/main/log/run.log 2>&1&
```
5. Specify the parameters and file paths in [`race_detection_main.py`](PythonCode/race_detection_main.py) and run the script in the console using the command below. To test your configuration, set the file paths accordingly to use the artificial test data provided [here](ArtificialTestData).  Please refer to Section 7 of the ABO Code and Data Appendix. 
```
nohup python3 path/to/code/race_detection_main.py > path/to/main/log/run.log 2>&1&
```
6. Use [`log_monitor.py`](PythonCode/log_monitor.py) to make sure all data have been processed. Follow the instructions in `log_monitor.py` and execute the code interactively or in a console.
```
python3 path/to/code/log_monitor.py
``` 
7. Specify the parameters and file paths in [`MainResults.R`](RCode/MainResults.R) and [`Sensitivity.R`](RCode/Sensitivity.R) and source the scripts. If you have multiple sets of race detection parameters, you may need to repeat this step multiple times for different sets of race detection results.
```
Rscript /path/to/MainResults.R
Rscript /path/to/RCode.R
```

## Feedback 

We would be grateful for feedback or comments on the code, and are especially eager to hear from early users. Please address comments, questions, and any other feedback to [eric.budish@chicagobooth.edu](mailto:eric.budish@chicagobooth.edu) and [hft.races.code.package@gmail.com](mailto:hft.races.code.package@gmail.com).

## Credits 

Code credits: Jiahao Chen, Natalia Drozdoff, Matthew O'Keefe, Jaume Vives, Zizhe Xia, Matteo Aquilina, Eric Budish, Peter O'Neill 

Documentation credits: Jiahao Chen, Zizhe Xia, Matteo Aquilina, Eric Budish, Peter O'Neill

## License

The Python code and documentation are licensed under the BSD 3-Clause license. License is available [here](LICENSE).

The R code is licensed under the GNU General Public License version 3 due to the use of GNU GPL licensed R packages. License is available [here](RCode/LICENSE-RCode)
