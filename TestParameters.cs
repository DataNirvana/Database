using DataNirvana.DomainModel.Database;
using MGL.Data.DataUtilities;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

//--------------------------------------------------------------------------------------------------------------------------------------------------------------------------
namespace DataNirvana.Database {
    //------------------------------------------------------------------------------------------------------------------------------------------------------------------
    /// <summary>
    ///     Name:      TestParameters
    ///     Author:     Edgar Scrase
    ///     Date:       July 2016
    ///     Version:    0.1
    ///     Description:
    ///                     A summary class to store the parameters for testing Redis and MySQL databases and to generate the random data.
    ///                     
    ///     =+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+
    ///     Copyright 2016 Data Nirvana Limited
    ///                     Licensed under the Apache License, Version 2.0 (the "License");
    ///                     you may not use this file except in compliance with the License.
    ///                     You may obtain a copy of the License at
    ///                     http://www.apache.org/licenses/LICENSE-2.0
    ///         
    ///                     Unless required by applicable law or agreed to in writing, software
    ///                     distributed under the License is distributed on an "AS IS" BASIS,
    ///                     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    ///                     See the License for the specific language governing permissions and
    ///                     limitations under the License.
    ///     =+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+
    /// </summary>
    public class TestParameters {

        //--------------------------------------------------------------------------------------------------------------------------------------------------------------------
        // Monitoring times
        public static TimeSpan WriteRedis = new TimeSpan();
        public static TimeSpan WriteMySQL = new TimeSpan();
        public static TimeSpan ReadRedis = new TimeSpan();
        public static TimeSpan ReadRedisCSharp = new TimeSpan();
        public static TimeSpan ReadRedisThreaded = new TimeSpan();
        public static TimeSpan ReadMySQL = new TimeSpan();

        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>
        ///     Whether to write the data or not
        /// </summary>
        public static bool DoWrite = true; //false; // true; // false;

        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        public static List<List<DatabaseSearchPattern>> PatternsToSearchFor = new List<List<DatabaseSearchPattern>>();
        public static int NumIterations = 3000000; // 2000000; //1000000; // 5000; //1000000; // 5000; //100000; // 5000; //1000000;
        public static int NumSearchPatterns = 10000;
        public static int SearchRangeSize = 5000;  // For integers - the other ranges are scaled from this using this as a ratio of the MaxInteger in the range
        /// <summary>
        ///     The maximum integer that will be generated in the random objects or in the search parameters.
        /// </summary>
        public static int MaxIntegerInRange = 1000000;
        public static long MinLongInRange = 8000000000;
        public static long MaxLongInRange = 12000000000;
        public static double MinDoubleInRange = 0;
        public static double MaxDoubleInRange = 1;

        public static DateTime DateMin = new DateTime(2016, 7, 1);
        public static DateTime DateMax = new DateTime(2016, 7, 31, 23, 59, 59);

        // This should be generated on the fly like the other calculations (the actual number is around 3.6 hrs)
        //public static int DateRangeInHours = 8;

        //public static bool UseThreading = false;
        public static bool UseThreading = true;

        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>
        ///     Stop out-of-memory exceptions ...
        /// </summary>
        public static int MaxNumObjects = 1000000;

        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>
        ///     For storing the randomly generated objects to be written
        /// </summary>
        public static List<object> RandomObjects = null;

        //--------------------------------------------------------------------------------------------------------------------------------------------------------------        
        /// <summary>
        ///     For storing the results of threaded READ testing
        /// </summary>
        public static List<TestObj> AllObjects = null;

        //--------------------------------------------------------------------------------------------------------------------------------------------------------------        
        /// <summary>
        ///     Redis configuration settings  
        ///     The first port MUST be the master, if there is slave replication.
        ///     If this is a cluster, all master nodes should be listed ...
        /// </summary>
        public static List<string> RedisHostWithPorts = new List<string> {
            "localhost:6379", "localhost:6380", "localhost:6381", "localhost:6382", "localhost:6383", "localhost:6384", "localhost:6385", "localhost:6386" };
//            "localhost:6379" };
        /// <summary>
        ///     The Redis hostname 
        /// </summary>
        public static string RedisHostName = "localhost";
        /// <summary>
        ///     The Redis auth keystring
        /// </summary>
        public static string RedisAuth = "iut5bfERJzQyckS2QVQC";
        /// <summary>
        ///     Whether or not we are using clustering... 
        /// </summary>
        public static bool RedisClusterMode = true; // false;
        /// <summary>
        ///     The specific Redis database to use
        /// </summary>
        public static int RedisDatabase = (RedisClusterMode == true) ? 0 : 1;




        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>
        ///     Generates a number of randomly generated RedisTestObj objects, with each parameter between a range...
        ///     These are ONLY used for writing, so we can ignore if the readonly flag is enabled
        /// </summary>
        public static void GenerateRandomObjects() {

            // These are ONLY used for writing, so we can ignore if the readonly flag is enabled
            if (TestParameters.DoWrite == false) {
                Logger.Log("Skipping generating the random objects as the global TestParameter.JustDoRead parameter is set to read only.");
                return;
            }

            RandomObjects = new List<object>();

            // Generate some random data!
            Logger.LogSubHeading("Generating " + NumIterations + " randomly generated TestObj objects...");

            int i = 0;
            for (i = 0; i < NumIterations; i++) {
                RandomObjects.Add(
                    // The primary key
                    new TestObj((ulong)i,
                    // A random int
                    MGLEncryption.GenerateRandomInt(1, MaxIntegerInRange),
                    // longs ...
                    MGLEncryption.GenerateRandomLong(TestParameters.MinLongInRange, TestParameters.MaxLongInRange),
                    // doubles ...
                    MGLEncryption.GenerateRandomDouble(TestParameters.MinDoubleInRange, TestParameters.MaxDoubleInRange),
                    // bools ...
                    MGLEncryption.GenerateRandomBool(),
                    // strings ...
                    MGLEncryption.GetSalt(20).ToString(),
                    // DateTime - Standardise the dates to the nearest second so that the MySQL vs Redis comparison is consistent
                    // Otherwise Redis is stored to the nearest MS while MySQL only stores to the nearest second
                    DatabaseSearchPattern.StandardiseDateToStartOfSecond(MGLEncryption.GenerateRandomDateTime(TestParameters.DateMin, TestParameters.DateMax), false)));

                // Log the progress ...
                Logger.Log(i, 1000, NumIterations);
            }
            Logger.Log(i, 1, NumIterations);
            Logger.Log("\n");

        }


        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>
        ///     Generates n search patterns with between 1 and 3 search parameters randomly chosen from 6 of the RedisTestObj attributes
        ///     11-Aug-2016 - updated so that the patterns we are searching for are broader if there are multiple tests ...
        ///     16-Aug-2016 - No longer searching using the TestBool parameter as the query results are far too broad.
        /// </summary>
        public static void GenerateRandomPatterns(List<DatabaseIndexInfo> riis) {

            //-----0----- Reset the list of patterns to search for
            PatternsToSearchFor = new List<List<DatabaseSearchPattern>>();

            //     1=PrimaryKey, 2=Integer, 3=Long, 4=Double, 5=String, 6=DateTime (Note we are not using bool searches here as they are far too sloooow)
            int totalNumPatterns = 6;

            //-----1----- Process the number of search pattern groups specified in the global parameter
            for (int i = 0; i < NumSearchPatterns; i++) {

                List<DatabaseSearchPattern> rsps = new List<DatabaseSearchPattern>();

                //-----2----- Determine how many tests to run and from which parameters...
                int numTests = MGLEncryption.GenerateRandomInt(1, 3);
                List<int> testTypes = new List<int>();
                for (int j = 0; j < numTests; j++) {
                    int testType = MGLEncryption.GenerateRandomInt(1, totalNumPatterns);

                    //-----2a----- Reduce the number of PK queries in the multiple search parameter queries and instead choose integer range parameters..
                    if (testType == 1 && j > 0) {
                        testType = 2;
                    }

                    //-----2b----- If this test type already exists, then iterate through and find a test type that has not yet already been chosen
                    while (testTypes.Contains(testType) == true) {
                        testType = MGLEncryption.GenerateRandomInt(1, totalNumPatterns);
                        // Reduce the number of PK queries in the multiple search parameter queries and instead choose integer range parameters..
                        if (testType == 1 && j > 0) {
                            testType = 2;
                        }
                    }
                    testTypes.Add(testType);


                    //-----3----- Now go through and randomly generate the SearchPattern for each of these test types
                    // We now have about six types of test we are processing...
                    if (testType == 1) {                   //-----a----- ID - Primary Key - Equivalent - individual result ...
                        rsps.Add(GenerateSearchPatternPrimaryKey(0, TestParameters.NumIterations));

                    } else if (testType == 2) {         //-----b----- TestInt  - Try to ensure that max results returned is 20%
                        rsps.Add(GenerateSearchPatternInteger(1, TestParameters.MaxIntegerInRange, numTests));

                    } else if (testType == 3) {         //-----c----- TestLong - a range of 4 billion and we are searching between for ranges of 10,000,000 (i.e. 0.25%)
                        rsps.Add(GenerateSearchPatternLong(TestParameters.MinLongInRange, TestParameters.MaxLongInRange, numTests));

                    } else if (testType == 4) {        //-----d----- TestDouble - Try to ensure that max results return is 20%
                        rsps.Add(GenerateSearchPatternDouble(TestParameters.MinDoubleInRange, TestParameters.MaxDoubleInRange, numTests));

                    } else if (testType == 5) {       //-----e----- TestStr - max results returned is 1/(40*40) when using a two char search length
                        rsps.Add(GenerateSearchPatternString());

                    } else if (testType == 6) {      //-----f----- TestDT - searching for ranges in hours
                        rsps.Add(GenerateSearchPatternDateTime(TestParameters.DateMin, TestParameters.DateMax, numTests));
                    }
                }

                // 19-Aug-2016 - if DoDebug == true, we need to sort here so that the views in the MySQL and Redis debug lists are consistent!
                // Otherwise it is difficult to compare the results of the two databases afterwards ..
                if ( RedisWrapper.DoDebug == true) {
                    rsps.Sort(DatabaseSearchPattern.Sort(riis));
                }

                // add our new search patterns ...
                PatternsToSearchFor.Add(rsps);
            }
        }


        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>
        ///     Generates a series of simple one pattern range searches which is useful for more specific testing
        ///     1=PrimaryKey, 2=Integer, 3=Long, 4=Double, 5=String, 6=DateTime (7=Bool -- Warning this one is sloooow)
        /// </summary>
        public static void GenerateRandomSingleSearchPatterns( int parameterToSearchWith) {

            // Lets generate some random integers ...
            PatternsToSearchFor = new List<List<DatabaseSearchPattern>>();

            for (int i = 0; i < TestParameters.NumSearchPatterns; i++) {

                DatabaseSearchPattern rsp = null;

                if (parameterToSearchWith == 1) {               // Primary key search
                    rsp = GenerateSearchPatternPrimaryKey(0, NumIterations);

                } else if (parameterToSearchWith == 2) {     // Integer search
                    rsp = GenerateSearchPatternInteger(1, TestParameters.MaxIntegerInRange, 1);

                } else if (parameterToSearchWith == 3) {    // Long search
                    rsp = GenerateSearchPatternLong(TestParameters.MinLongInRange, TestParameters.MaxLongInRange, 1);

                } else if (parameterToSearchWith == 4) {    // Double search
                    rsp = GenerateSearchPatternDouble(TestParameters.MinDoubleInRange, TestParameters.MaxDoubleInRange, 1);

                } else if (parameterToSearchWith == 5) {    // String search
                    rsp = GenerateSearchPatternString();

                } else if (parameterToSearchWith == 6) {    // DateTime search
                    rsp = GenerateSearchPatternDateTime(TestParameters.DateMin, TestParameters.DateMax, 1);

                } else if (parameterToSearchWith == 7) {    // Bool search
                    Logger.LogWarning("TestParameters.GenerateRandomIntegerRangePatterns - Boolean searches are SLOOOW and deprecated - be careful.");

                    TestParameters.PatternsToSearchFor.Add(new List<DatabaseSearchPattern>() {
                        new DatabaseSearchPattern("TestBool", MGLEncryption.GenerateRandomBool())
                    });


                } else {
                    Logger.LogWarning("TestParameters.GenerateRandomIntegerRangePatterns - Unknown search parameter requested - " + parameterToSearchWith);                       
                }

                // And finally - lets add our search pattern ...
                TestParameters.PatternsToSearchFor.Add(new List<DatabaseSearchPattern>() { rsp });
            }
        }


        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        public static DatabaseSearchPattern GenerateSearchPatternPrimaryKey(long start, long end) {
            // We just generate a random number between the start and the end
            return new DatabaseSearchPattern(MGLEncryption.GenerateRandomLong(start, end));
        }


        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        public static DatabaseSearchPattern GenerateSearchPatternInteger(int min, int max, int numTestsInGroup) {
            DatabaseSearchPattern rsp = null;

            //-----a----- Get the expression type (==, <=, >= or between)
            int randomET = GenerateExpressionType(numTestsInGroup);

            //-----b----- Generate the range
            int range = TestParameters.SearchRangeSize; // 50,000

            //-----c----- We want to generalise for multiple tests --- so if numTests > 0, then increase the range and bring in the min and max to ensure a larger range .....
            if (numTestsInGroup > 1) {
                range = range * 10;
                // For >= or <= lets also tweak the min and max to ensure the range is larger
                if (randomET == 2) {
                    min = min + range;
                    max = max - range;
                }
            }

            //-----d----- Generate the random value
            int randomVal = MGLEncryption.GenerateRandomInt(min, max);

            //-----e----- Now generate the search pattern
            if (randomET == 1) {           //_____ == _____
                rsp = new DatabaseSearchPattern("TestInt", randomVal);

            } else if (randomET == 2) {   //_____ >= or <= range search _____
                bool randomBool = MGLEncryption.GenerateRandomBool();

                if (randomBool == true) {
                    rsp = new DatabaseSearchPattern("TestInt", MGLEncryption.GenerateRandomInt(max - range, max), randomBool);
                } else {
                    rsp = new DatabaseSearchPattern("TestInt", MGLEncryption.GenerateRandomInt(min, min + range), randomBool);
                }

            } else {                               //_____ Between _____
                rsp = new DatabaseSearchPattern("TestInt", randomVal, randomVal + range);

            }

            return rsp;
        }


        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        public static DatabaseSearchPattern GenerateSearchPatternLong(long min, long max, int numTestsInGroup) {
            DatabaseSearchPattern rsp = null;

            //-----a----- Get the expression type (==, <=, >= or between)
            int randomET = GenerateExpressionType(numTestsInGroup);

            //-----b----- The range is 20m by default
            long range = ((max - min) * TestParameters.SearchRangeSize / TestParameters.MaxIntegerInRange);

            //-----c----- So we want to generalise for multiple tests --- so if numTests > 0, then increase the range .....
            if (numTestsInGroup > 1) {
                // increase the range by a factor of 10 ...
                range = range * 10;
                // For >= or <= lets also tweak the min and max to ensure the range is larger
                if (randomET == 2) {
                    min = min + range;
                    max = max - range;
                }
            }

            //-----d----- Generate the random value
            long randomVal = MGLEncryption.GenerateRandomLong(min, max);

            //-----e----- Generate the search expression
            if (randomET == 1) {           //_____ == _____
                rsp = new DatabaseSearchPattern("TestLong", randomVal);

            } else if (randomET == 2) {   //_____ >= or <= range search _____

                // Work out if this is a <= or >= query
                bool randomBool = MGLEncryption.GenerateRandomBool();

                if (randomBool == true) {
                    rsp = new DatabaseSearchPattern("TestLong", MGLEncryption.GenerateRandomLong(max - range, max), randomBool);
                } else {
                    rsp = new DatabaseSearchPattern("TestLong", MGLEncryption.GenerateRandomLong(min, min + range), randomBool);
                }

            } else {                               //_____ Between _____
                rsp = new DatabaseSearchPattern("TestLong", randomVal, randomVal + range);
            }

            return rsp;
        }


        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        public static DatabaseSearchPattern GenerateSearchPatternDouble(double min, double max, int numTestsInGroup) {
            DatabaseSearchPattern rsp = null;

            //-----a----- Get the expression type (==, <=, >= or between)
            int randomET = GenerateExpressionType(numTestsInGroup);

            //-----b----- Generate the default range size
            double range = (double)TestParameters.SearchRangeSize / (double)TestParameters.MaxIntegerInRange;

            //----c----- so we want to generalise for multiple tests --- so if numTests > 0, then increase the range and bring in the min and max to ensure a larger range .....           
            if (numTestsInGroup > 1) {
                range = range * 4;
                // For >= or <= lets also tweak the min and max to ensure the range is larger
                if (randomET == 2) {
                    min = min + range;
                    max = max - range;
                }
            }

            //-----d----- Generate the random value
            double randomVal = MGLEncryption.GenerateRandomDouble(min, max);

            //-----e----- Generate the search pattern
            if (randomET == 1) {              //_____ == _____
                rsp = new DatabaseSearchPattern("TestDouble", randomVal);

            } else if (randomET == 2) {     //_____ >= or <= range search _____

                // Work out if this is a <= or >= query
                bool randomBool = MGLEncryption.GenerateRandomBool();

                if (randomBool == true) {
                    rsp = new DatabaseSearchPattern("TestDouble", MGLEncryption.GenerateRandomDouble(max - range, max), randomBool);
                } else {
                    rsp = new DatabaseSearchPattern("TestDouble", MGLEncryption.GenerateRandomDouble(min, min + range), randomBool);
                }

            } else {                               //_____ Between _____
                rsp = new DatabaseSearchPattern("TestDouble", randomVal, randomVal + range);

            }

            return rsp;
        }


        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        public static DatabaseSearchPattern GenerateSearchPatternString() {
            return new DatabaseSearchPattern("TestStr", MGLEncryption.GetSalt(RedisWrapper.TextIndexLength).ToString().ToLower());
        }

        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        public static DatabaseSearchPattern GenerateSearchPatternDateTime(DateTime min, DateTime max, int numTestsInGroup) {
            DatabaseSearchPattern rsp = null;

            //-----a----- Get the expression type (==, <=, >= or between)
            int randomET = GenerateExpressionType(numTestsInGroup);

            //-----b----- The default range in HOURS - note that we calculate this in the same manner as the other ranges from the integer parameter (approx 3.6 hrs)
            int range = (int) Math.Round(TestParameters.DateMax.Subtract(TestParameters.DateMin).TotalHours * TestParameters.SearchRangeSize / TestParameters.MaxIntegerInRange);

            //----c----- so we want to generalise for multiple tests --- so if numTests > 0, then increase the range and bring in the min and max to ensure a larger range .....           
            if (numTestsInGroup > 1) {
                range = range * 18; // becomes roughly three days // 8;
                // For >= or <= lets also tweak the min and max to ensure the range is larger
                if (randomET == 2) {
                    min = min.AddHours(range);
                    max = max.AddHours(-range);
                }
            }

            //-----d----- Generate the random value
            DateTime randomVal = DatabaseSearchPattern.StandardiseDateToStartOfSecond(MGLEncryption.GenerateRandomDateTime(min, max), false);

            //-----e----- And generate the search pattern as well...
            if (randomET == 1) {            //_____ == _____
                                            
                // Note that for dates equal to - would result in all the results for a specific date and time being returned - which is only very rarely going to return results
                rsp = new DatabaseSearchPattern("TestDT", randomVal);

            } else if (randomET == 2) {     //_____ >= or <= range search _____

                // We need a random bool to determine if this is a >= or a <= search ...
                bool randomBool = MGLEncryption.GenerateRandomBool();

                // Generate ranges between the temporarily defined max and min above and a range's width above or below these parameters
                // depending on whether this is greater than or less than ...
                if (randomBool == true) {
                    randomVal = DatabaseSearchPattern.StandardiseDateToStartOfSecond(MGLEncryption.GenerateRandomDateTime(max.AddHours(-range), max), false);
                } else {
                    randomVal = DatabaseSearchPattern.StandardiseDateToStartOfSecond(MGLEncryption.GenerateRandomDateTime(min, min.AddHours(range)), false);
                }

                // Now add this randomVal as a range query
                rsp = new DatabaseSearchPattern("TestDT", randomVal, randomBool);

            } else {                            //_____ Between _____

                // Between our randomly generated date and a range's width above this ...
                rsp = new DatabaseSearchPattern("TestDT", randomVal, randomVal.AddHours(range));

            }

            return rsp;
        }


        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>
        ///     Generates a random number between 1 and 10; normally 1 will be used for equivalence, 2 for less than or greater than and the remainder for between.
        ///     A special case arises with the equivalence - we only want to use this if there is only one test (this one) in the group of search patterns
        /// </summary>
        public static int GenerateExpressionType(int numTestsInGroup) {
            // We will need to randomly choose the expression type =, >=, <=, BETWEEEN, with only a few results as equivalent
            // For most of the range queries 1 will be equivalence (and only if there is only one search pattern), 2 is <= or >= and anything else is between...
            // We want to weight heavily to the between queries as these are truely random and will pull out results from the full range, rather than just the top and tail ..
            int randomET = MGLEncryption.GenerateRandomInt(1, 10);

            // If there are more than one query do not go for the equivalence as these will generate too few results...
            if (randomET == 1 && numTestsInGroup > 1) {
                while (randomET == 1) {
                    randomET = MGLEncryption.GenerateRandomInt(1, 10);
                }
            }

            return randomET;
        }

    }
}
