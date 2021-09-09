# HFT-Races

This repository contains code for researchers, regulators or practitioners who wish to use financial-exchange message data to quantify latency arbitrage and study other aspects of speed-sensitive trading, following Matteo Aquilina, Eric Budish and Peter O’Neill, [“Quantifying the High-Frequency Trading ‘Arms Race’”](https://faculty.chicagobooth.edu/eric.budish/research/Quantifying-HFT-Races.pdf), Quarterly Journal of Economics, 2021 (hereafter, “ABO”). The Python code processes the user's message data, detects trading races, and outputs a race-level statistical dataset along with complementary trading data. The R code produces race summary statistics, tables and figures analogous to all reported results in ABO. We also provide a small artificial data set that can be used to understand the data structure and to test one's configuration.

This code should be used in conjunction with the detailed documentation linked below. 

## About

Version 1.0 (September 2021). Please visit [https://github.com/ericbudish/HFT-Races](https://github.com/ericbudish/HFT-Races) to check for updates. 

## Documentation

Please refer to [ABO Code and Data Appendix](Code_and_Data_Appendix.pdf) for detailed documentation and instructions.

## Feedback 

We would be grateful for feedback or comments on the code, and are especially eager to hear from early users. Please address comments, questions, and any other feedback to [eric.budish@chicagobooth.edu](mailto:eric.budish@chicagobooth.edu) and [hft.races.code.package@gmail.com](mailto:hft.races.code.package@gmail.com).

## Credits 

Code credits: Jiahao Chen, Natalia Drozdoff, Matthew O'Keefe, Jaume Vives, Zizhe Xia, Matteo Aquilina, Eric Budish, Peter O'Neill 

Documentation credits: Jiahao Chen, Zizhe Xia, Matteo Aquilina, Eric Budish, Peter O'Neill

## License

The Python code and documentation are licensed under the BSD 3-Clause license. License is available [here](licenses/LICENSE).

The R code is licensed under the GNU General Public License version 3 due to the use of GNU GPL licensed R packages. License is available [here](licenses/LICENSE-RCode)
