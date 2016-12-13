using DataNirvana.DomainModel.Database;
using MGL.Data.DataUtilities;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

//--------------------------------------------------------------------------------------------------------------------------------------------------------------------
namespace DataNirvana.Database {
    //-----------------------------------------------------------------------------------------------------------------------------------------------------------------
    public class TestingMySQL {

        // clean up the static variables to minimise memory usage
        static HashSet<uint> IDs = new HashSet<uint>();
        static List<TestObj> TestObjects = new List<TestObj>();

        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        public static void RunMySQLSearchingTest() {
            //-----1----- MySQL
            Logger.LogSubHeading("Now testing the MySQL to get a benchmark ...");
            DateTime MySQLTest = DateTime.Now;

            if (TestParameters.UseThreading == false) {
                PerformanceTestMySQLThreadBlock(TestParameters.NumSearchPatterns, 0);
            } else {
                //bool success = DNThreadManager.Run("RedisRead", NumIterations, 2500, new Action<int, int>( PerformanceTestRedisRead));
                bool success = DNThreadManager.Run("MySQLRead", TestParameters.NumSearchPatterns, PerformanceTestMySQLThreadBlock);
            }

            TimeSpan MySQLTested = DateTime.Now.Subtract(MySQLTest);
            Logger.Log("");
            Logger.Log("Time to process:" + MySQLTested.TotalSeconds);

            Logger.Log("Found " + IDs.Count + " objects matching one or more of the search patterns ...");

            Logger.Log("MySQL read testing completed");

            // clean up the static variables to minimise memory usage
            IDs = new HashSet<uint>();

        }


        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        public static void PerformanceTestMySQLWrite() {

            Logger.LogSubHeading("Testing MySQL!");
            bool success = false;

            // Now lets connect to the MySQL db
            Logger.Log("Connecting to the database...");
            DatabaseWrapper dbInfo = new DatabaseWrapper(new DatabaseConnectionInfo("localhost", "mysql", "redis_comparison",
                SecureStringWrapper.Encrypt("forum"), SecureStringWrapper.Encrypt(TestParameters.RedisAuth), 3306));
            dbInfo.Connect();

            if (TestParameters.DoWrite == true) {
                bool tnExists = dbInfo.TableExists(TestObj.ObjectName);

                // Drop the table if it exists already ...
                bool tnDeleted = dbInfo.DeleteTable(TestObj.ObjectName);

                // And then recreate it ...
                // 22-Aug-2016 - Remove the boolean index to mirror the Redis implementation - Index TestBool(TestBool),
                bool tnCreated = dbInfo.CreateTable(TestObj.ObjectName, @"
                ID bigint unsigned,	    Primary Key(ID),
                TestInt int,    		        Index TestInt(TestInt),
                TestLong bigint,  		Index TestLong(TestLong),
                TestDouble double,  	Index TestDouble(TestDouble),
                TestBool bool,    		
                TestStr Varchar(20),    Index TestStr(TestStr),
                TestDT DateTime,    	Index TestDT(TestDT)
            ", false);
            }

            Logger.Log("Writing data");
            DateTime writeStart = DateTime.Now;

            if (TestParameters.DoWrite == true) {
                int writeCount = 0;
                foreach (object o in TestParameters.RandomObjects) {
                    TestObj rto = (TestObj)o;

                    StringBuilder sql = new StringBuilder();

                    sql.Append("INSERT INTO "+ TestObj.ObjectName + " (ID, TestInt, TestLong, TestDouble, TestBool, TestStr, TestDT) VALUES (");
                    sql.Append(rto.ID + ",");
                    sql.Append(rto.TestInt + ",");
                    sql.Append(rto.TestLong + ",");
                    sql.Append(rto.TestDouble + ",");
                    sql.Append(rto.TestBool + ",");
                    sql.Append(DataUtilities.Quote(rto.TestStr) + ",");
                    sql.Append(DataUtilities.Quote(DateTimeInformation.FormatDatabaseDate(rto.TestDT, true, true)));
                    sql.Append(");");
                    dbInfo.ExecuteSQL(sql.ToString(), ref success);

                    Logger.Log(++writeCount, 100, TestParameters.RandomObjects.Count);
                }

            }
            Logger.Log("");
            Logger.Log("Writing completed.");
            TestParameters.WriteMySQL = DateTime.Now.Subtract(writeStart);

            Logger.Log("Disconnect from the database.");
            dbInfo.Disconnect();

        }


        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        public static void PerformanceTestMySQLRead() {

            Logger.LogSubHeading("Testing MySQL Threaded!");

            bool success = false;

            Logger.Log("Now starting to test the patterns");
            DateTime readStart = DateTime.Now;

            if (TestParameters.UseThreading == false) {
                PerformanceTestMySQLThreadBlock(TestParameters.NumSearchPatterns, 0);
            } else {
                //bool success = DNThreadManager.Run("RedisRead", NumIterations, 2500, new Action<int, int>( PerformanceTestRedisRead));
                success = DNThreadManager.Run("MySQLRead", TestParameters.NumSearchPatterns, PerformanceTestMySQLThreadBlock);
            }

            Logger.Log("Found " + IDs.Count + " objects matching one or more of the search patterns ...");

            TestParameters.ReadMySQL = DateTime.Now.Subtract(readStart);
            Logger.Log("MySQL read testing completed");

            // clean up the static variables to minimise memory usage
            IDs = new HashSet<uint>();


        }

        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>
        ///     So this loads the full object for each search match found ... but does not persist those as that would likely kill the memory allocation
        /// </summary>
        /// <param name="chunkSize"></param>
        /// <param name="startIndex"></param>
        public static void PerformanceTestMySQLThreadBlock(int chunkSize, int startIndex) {

            Logger.LogSubHeading("Testing MySQL!");
            //bool success = false;

            int i = startIndex;
            if ((startIndex + chunkSize) >= TestParameters.PatternsToSearchFor.Count) {
                chunkSize = TestParameters.PatternsToSearchFor.Count - startIndex;
            }

            Logger.Log("Starting chunk with index " + startIndex + " and size " + chunkSize + " ...");

            
//            HashSet<ulong> ids = new HashSet<ulong>();

            // Now lets connect to the MySQL db
            Logger.Log("Connecting to the database...");
            DatabaseWrapper dbInfo = new DatabaseWrapper(new DatabaseConnectionInfo("localhost", "mysql", "redis_comparison",
                SecureStringWrapper.Encrypt("forum"), SecureStringWrapper.Encrypt(TestParameters.RedisAuth), 3306));
            dbInfo.Connect();


            Logger.Log("Reading data");
            StringBuilder log = new StringBuilder();
            StringBuilder queryTxt = new StringBuilder();

            //long searchIndex = startIndex;

            for (i = startIndex; i < (startIndex + chunkSize); i++) {
                List<DatabaseSearchPattern> rsps = TestParameters.PatternsToSearchFor[i];

                /////////////
                //bool storeQuery = false;

                StringBuilder sql = new StringBuilder();
                StringBuilder searchParams = new StringBuilder();


                // And build the query here ...
                sql.Append("SELECT ID, TestInt, TestLong, TestDouble, TestBool, TestStr, TestDT FROM "+ TestObj.ObjectName + " WHERE ");
                foreach (DatabaseSearchPattern rsp in rsps) {
                    if (searchParams.Length > 0) {
                        searchParams.Append(" AND ");
                    }

                    if (rsp.SearchType == SearchType.PrimaryKey) {
                        searchParams.Append(" " + rsp.Parameter + "=" + rsp.Score);
                    } else if (rsp.SearchType == SearchType.Text) {
                        searchParams.Append(" " + rsp.Parameter + " LIKE " + DataUtilities.Quote(rsp.PatternStartsWith(RedisWrapper.TextIndexLength) + "%"));
                        //searchParams.Append(" " + rsp.Parameter + " LIKE " + DataUtilities.Quote(rsp.Pattern.Substring(0, 1) + "%"));
                    } else if (rsp.SearchType == SearchType.Bool) {
                        searchParams.Append(" " + rsp.Parameter + "=" + rsp.Score);
                    } else if (rsp.SearchType == SearchType.DateTime) {
                        // could also add switches on ==, >= and <=
                        if (rsp.SearchComparisonExpression == SearchComparisonExpression.Equivalent) {
                            //if (rsp.ScoreMin == rsp.ScoreMax) {                                    // an exact date and time
                            searchParams.Append(rsp.Parameter + " = "
                                + DataUtilities.Quote(DateTimeInformation.FormatDatabaseDate(new DateTime(rsp.Score), true, true)));

                        } else if (rsp.SearchComparisonExpression == SearchComparisonExpression.RangeLessThanOrEqualTo) {
                            //} else if (rsp.ScoreMin == DateTime.MinValue.Ticks) {           
                            // less than or equal to ...                            
                            //                            searchParams.Append(rsp.Parameter + " <= "
                            //                                + DataUtilities.Quote(DateTimeInformation.FormatDatabaseDate(new DateTime((long)Math.Round(rsp.ScoreMax)), true, true)));
                            // Note that we want to search inclusively.  Which means we need to search less than the specified max day PLUS A DAY
                            // equivalent to <= 2016-04-20 23:59:59 to ensure that the day itself is included ....

                            //DateTime dt = new DateTime((long)Math.Round(rsp.ScoreMax));
                            DateTime dt = new DateTime((long)rsp.ScoreMax);

                            searchParams.Append(rsp.Parameter + " <= "
                                + DataUtilities.Quote(DateTimeInformation.FormatDatabaseDate(dt, true, true)));

                            /////////////////////////////////////
                            //string checkTHIS = "";

                        } else if (rsp.SearchComparisonExpression == SearchComparisonExpression.RangeGreaterThanOrEqualTo) {
                            //} else if (rsp.ScoreMax == DateTime.MaxValue.Ticks) {           // greater than or equal to ...

                            //DateTime dt = new DateTime((long)Math.Round(rsp.ScoreMin));
                            DateTime dt = new DateTime((long)rsp.ScoreMin);

                            searchParams.Append(rsp.Parameter + " >= "
                                + DataUtilities.Quote(DateTimeInformation.FormatDatabaseDate(dt, true, true)));

                            /////////////////////////////////////
                            //string checkTHIS = rsp.AsText();
                            //checkTHIS = rsp.AsText();
                            //storeQuery = true;
                        } else if (rsp.SearchComparisonExpression == SearchComparisonExpression.RangeBetween) {
                            //                        } else {                                                                            // Must be a real between

                            // Note that we want to search inclusively.  Which means we need to search less than the specified max day PLUS A DAY
                            // equivalent to <= 2016-04-20 23:59:59 to ensure that the day itself is included ....
                            searchParams.Append(rsp.Parameter + " BETWEEN "
                                //+ DataUtilities.Quote(DateTimeInformation.FormatDatabaseDate(new DateTime((long)Math.Round(rsp.ScoreMin)), true, true))
                                + DataUtilities.Quote(DateTimeInformation.FormatDatabaseDate(new DateTime((long)rsp.ScoreMin), true, true))
                                + " AND "
                                //+ DataUtilities.Quote(DateTimeInformation.FormatDatabaseDate(new DateTime((long)Math.Round(rsp.ScoreMax)), true, true)));
                                + DataUtilities.Quote(DateTimeInformation.FormatDatabaseDate(new DateTime((long)rsp.ScoreMax), true, true)));
                        } else {
                            string ohFuck = "";
                        }

                    } else if (rsp.SearchType == SearchType.Score) {
                        if (rsp.SearchComparisonExpression == SearchComparisonExpression.Equivalent) {
                        //if (rsp.ScoreMin == rsp.ScoreMax) {                                    // an exact value
                        searchParams.Append(rsp.Parameter + " = " + rsp.ScoreMin.ToString("0." + new string('#', 339)));

                        } else if (rsp.SearchComparisonExpression == SearchComparisonExpression.RangeLessThanOrEqualTo) {
                        //} else if (rsp.ScoreMin == Double.MinValue) {           // less than or equal to ...
                            searchParams.Append(rsp.Parameter + " <= " + rsp.ScoreMax.ToString("0." + new string('#', 339)));

                        } else if (rsp.SearchComparisonExpression == SearchComparisonExpression.RangeGreaterThanOrEqualTo) {
                        //} else if (rsp.ScoreMax == Double.MaxValue) {           // greater than or equal to ...
                            searchParams.Append(rsp.Parameter + " >= " + rsp.ScoreMin.ToString("0." + new string('#', 339)));

                        } else if (rsp.SearchComparisonExpression == SearchComparisonExpression.RangeBetween) {
                        //} else {                                                                            // Must be a real between
                            searchParams.Append(rsp.Parameter + " BETWEEN "
                            + rsp.ScoreMin.ToString("0." + new string('#', 339)) + " AND "
                            + rsp.ScoreMax.ToString("0." + new string('#', 339)));

                        } else {
                            string ohFuck = "";
                        }

                    }
                }
                sql.Append(searchParams);
                sql.Append(";");

                List<string[]> sqlData = dbInfo.GetDataList(sql.ToString());

                int objCount = 0;
                if (sqlData != null) {
                    objCount = sqlData.Count;

                    foreach (string[] row in sqlData) {
                        ulong id = 0;
                        ulong.TryParse(row[0], out id);

                        int testInt = 0;
                        int.TryParse(row[1], out testInt);

                        long testLong = 0;
                        long.TryParse(row[2], out testLong);

                        double testDouble = 0;
                        double.TryParse(row[3], out testDouble);

                        bool testBool = (row[4] != null && row[4].Equals("1") == true) ? true : false;

                        // string is obvs good to go!!

                        DateTime testDT = DateTimeInformation.FormatDateTime(row[6]);

                        TestObj rto = new TestObj(id, testInt, testLong, testDouble, testBool, row[5], testDT);

                        // Keep adding the objects, as long as they have not already been added and the MaxNumObjects is not exceeded - probably 1m
                        // Now append the ids to the global list ...
                        lock (IDs) {
                            if (IDs.Contains((uint)rto.ID) == false) {
                                IDs.Add((uint)rto.ID);
                                TestObjects.Add(rto);
                            }
                        }
                    }
                }


                // Do the printing of all the specific queries only if DoDebug is set to true ....
                if (RedisWrapper.DoDebug == true) {

                    // 19-Aug-2016 - Fuck a duck - DO NOT SORT THE QUERIES HERE
                    // We need to make sure not to sort the master list as it potentially confuses the calculations in the search objects method ...

                    queryTxt.Clear();
                    foreach (DatabaseSearchPattern rsp in rsps) {
                        queryTxt.Append(" " + rsp.AsText());
                    }

                    log.Append("\n" + "Search " + i + " found " + objCount + " objects. Query Text: " + queryTxt);
                }



                //foreach (RedisSearchPattern rsp in rsps) {
                //    queryTxt.Append(" " + rsp.AsText());
                //}

                ////log.Append("\n" + "Search " + (++searchIndex) + " found " + objCount + " objects. Query Text: " + queryTxt);
                //log.Append("\n" + "Search " + (++searchIndex) + " found " + objCount + " objects. Query Text: " + queryTxt + "\t" + sql);

                ////////////////////////////
                //if (storeQuery == true && (sqlData == null || sqlData.Count == 0)) {
                //    log.Append("\t" + sql);
                //}


                if (TestParameters.UseThreading == true) {
                    DNThreadManager.IncrementNumIterationsCompleted();
                } else {
                    Logger.Log(i, 10, TestParameters.PatternsToSearchFor.Count);
                }
            }


            Logger.Log("");
            Logger.Log(log.ToString());

            // And lastly, lets disconnect from the database!
            dbInfo.Disconnect();

        }



    }
}
