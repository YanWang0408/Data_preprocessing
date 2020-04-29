def reverseString(mystr):
    return mystr[::-1]
#Wrap the function and store as a variable
udfReverseString = udf(reverseString, StringType())
#use it with spark
user_df = user_df.withColumn('reverseName', udfReverseString(user_df.Name))



def getFirstAndMiddle(names):
  return ' '.join(names[0:2])

# Define the method as a UDF
udfFirstAndMiddle = F.udf(getFirstAndMiddle, StringType())

# Create a new column using your UDF
voter_df = voter_df.withColumn('first_and_middle_name', udfFirstAndMiddle(voter_df.splits))

# Drop the unnecessary columns then show the DataFrame
voter_df = voter_df.drop('first_name')
voter_df = voter_df.drop('splits')
voter_df.show()



