# Split the content of _c0 on the tab character (aka, '\t')
split_cols = F.split(annotations_df["_c0"], '\t')

# Add the columns folder, filename, width, and height
split_df = annotations_df.withColumn('folder', split_cols.getItem(0))
split_df = split_df.withColumn('filename', split_cols.getItem(1))
split_df = split_df.withColumn('width', split_cols.getItem(2))
split_df = split_df.withColumn('height', split_cols.getItem(3))

# Add split_cols as a column
split_df = split_df.withColumn('split_cols', split_cols)

'''
+-------------------------------------------------------------------------------------+--------+
|_c0                                                                                  |colcount|
+-------------------------------------------------------------------------------------+--------+
|02110627	n02110627_12938	200	300	affenpinscher,0,9,173,298                           |5       |
|02093754	n02093754_1148	500	378	Border_terrier,73,127,341,335                        |5       |
|%s	%s	800	600	Shetland_sheepdog,124,87,576,514                                       |5       |
|02104029	n02104029_63	500	375	kuvasz,0,0,499,327                                     |5       |
|02111500	n02111500_5137	500	375	Great_Pyrenees,124,225,403,374                       |5       |
|02104365	n02104365_7518	500	333	schipperke,146,29,416,309                            |5       |
|02105056	n02105056_2834	500	375	groenendael,168,0,469,374                            |5       |
|02093647	n02093647_541	500	333	Bedlington_terrier,10,12,462,332                      |5       |
|02098413	n02098413_1355	500	375	Lhasa,39,1,499,373                                   |5       |
|02093859	n02093859_2309	330	500	Kerry_blue_terrier,17,16,300,482                     |5       |
|02100583	n02100583_702	500	333	vizsla,112,93,276,236                                 |5       |
|02109961	n02109961_1017	475	500	Eskimo_dog,43,20,472,461                             |5       |
|02096177	n02096177_11642	500	375	cairn,71,2,319,302                                  |5       |
|02108000	n02108000_3491	600	450	EntleBucher,307,94,515,448	EntleBucher,101,33,330,448|6       |
|02085782	n02085782_1731	600	449	Japanese_spaniel,23,0,598,435                        |5       |
|02109047	n02109047_888	410	368	Great_Dane,51,36,355,332                              |5       |
|02110185	n02110185_2736	259	500	Siberian_husky,7,2,235,498                           |5       |
|02086646	n02086646_2970	500	400	Blenheim_spaniel,25,66,401,387                       |5       |
|02096177	n02096177_6751	500	375	cairn,82,2,472,369                                   |5       |
|02098413	n02098413_1713	500	333	Lhasa,141,40,423,185                                 |5       |
+-------------------------------------------------------------------------------------+--------+
only showing top 20 rows

'''

def retriever(cols, colcount):
  # Return a list of dog data
  return cols[4:colcount]

# Define the method as a UDF
udfRetriever = F.udf(retriever, ArrayType(StringType()))

# Create a new column using your UDF
split_df = split_df.withColumn('dog_list', udfRetriever(split_df.split_cols, split_df.colcount))

# Remove the original column, split_cols, and the colcount
split_df = split_df.drop('_c0').drop('split_cols').drop('colcount')