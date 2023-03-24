DIGen -  Data Generation utility v1.1.0

This data generation utility is used for generating sourece data the TPC-DI benchmark. 

========================================
 Dependencies:
 - Requires Java SE 7 or above
 - PDGF is located in the same directory

========================================
 Usage:
 java -jar DIGen.jar <options>

 <options>
  -h			Print usage information
  -sf <sf>		Set the scale factor to use when generating data. Default is 5. Range: 3 - 2147483647
  -o <dir>		Specify the directory where to generate the source data
  -v			Print the DIGen version
  -jvm <JVM options>	DIGen creates a separate JVM to perform the actual data generation. JVM options supplied here are given to this JVM. E.g. -jvm "-Xms1g -Xmx2g"
  -d			Debug

=========================================
 Reporting:
 A report file named digen_report.txt will be generated in the output directory. This report must not be modified. The report contains:
   - General information about the generation process (date, time, DIGen version)
   - Options used (scale factor, etc.)
   - Rows generated for each batch. This information is needed to calculate the benchmark performance metric. 


