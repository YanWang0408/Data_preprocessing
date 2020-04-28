import pyspark.sql.functions as F
# Add a new column called splits separated on whitespace
voter_df = voter_df.withColumn('splits', F.split(voter_df.VOTER_NAME, '\s+'))

# Create a new column called first_name based on the first item in splits
voter_df = voter_df.withColumn('first_name', voter_df.splits.getItem(0))

# Get the last entry of the splits list and create a column called last_name
voter_df = voter_df.withColumn('last_name', voter_df.splits.getItem(F.size('splits') - 1))

# Drop the splits column
voter_df = voter_df.drop('splits')

# Show the voter_df DataFrame
voter_df.show()



# Add a column to voter_df named random_val with the results of the F.rand() method for any voter with the title Councilmember.
# Add a column to voter_df for a voter based on their position
voter_df = voter_df.withColumn('random_val',
                                when(voter_df.TITLE == 'Councilmember', F.rand())
                               .when(voter_df.TITLE == 'Mayor', 2)
                               .otherwise(0))

# Use the .filter() clause with random_val
voter_df.filter(voter_df.random_val == 0).show()
