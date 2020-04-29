# Import the data to a DataFrame
departures_df = spark.read.csv('2015-departures.csv.gz', header=True)

# Remove any duration of 0
departures_df = departures_df.filter(departures_df[3] > 0)

# Add an ID column
departures_df = departures_df.withColumn('id', F.monotonically_increasing_id())

# Write the file out to JSON format
departures_df.write.json('output.json')


'''
After creating your quick pipeline, you provide the json file to an analyst on your team. 
After loading the data and performing a couple exploratory tasks, 
the analyst tells you there's a problem in the dataset while trying to sort the duration data. 
She's not sure what the issue is beyond the sorting operation not working as expected.

Date          Flight Number   Airport     Duration    ID

09/30/2015    2287            ANC         409         107962
12/28/2015    1408            OKC         41          141917
08/11/2015    2287            ANC         410         87978
'''

departures_df = departures_df.withColumn('Duration', departures_df['Duration'].cast(IntegerType()))