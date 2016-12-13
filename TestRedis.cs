using DataNirvana.DomainModel.Database;
using MGL.Data.DataUtilities;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

//--------------------------------------------------------------------------------------------------------------------------------------------------------------------
namespace DataNirvana.Database {
    //------------------------------------------------------------------------------------------------------------------------------------------------------------------
    /// <summary>
    ///     Name:      RedisTesting
    ///     Author:     Edgar Scrase
    ///     Date:       July and August 2016
    ///     Version:    0.1
    ///     Description:
    ///                     Tests the Redis object extraction and searching code
    ///                     There are two random methods, a simple search and a more detailed search, both normal and brute force
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
    public class TestRedis {

        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        public static RedisWrapper rw;

        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>
        ///     To support the threading ...
        /// </summary>
        static int NumCompleted = 0;

        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        // either ulongs or uints based on TestParameters.UseUIntID ...
        static HashSet<uint> IDs = new HashSet<uint>();

        //        static List<List<RedisSearchPattern>> PatternsToSearchFor = new List<List<RedisSearchPattern>>();
        //        static int NumIterations = 1000;
        //        static int NumSearchPatterns = 10000;


        // TestParameters.

        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        // The configuration ...
        //        public static List<string> RedisHostWithPorts = new List<string> {
        //            "localhost:6379", "localhost:6380", "localhost:6381", "localhost:6382", "localhost:6383", "localhost:6384", "localhost:6385", "localhost:6386" };
        //        public static string RedisHostName = "localhost";
        //        public static string RedisAuth = "iut5bfERJzQyckS2QVQC";
        //        public static bool RedisClusterMode = true;
        //        public static int RedisDatabase = (RedisClusterMode == true) ? 0 : 1;
        //        public static int RedisNumDBConnections = 10; // Anymore than this does not improve the performance significantly;

        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        public TestRedis() {

        }


        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        public static void StartRedisTesting() {

            Logger.Log("Connecting to the Redis database");
            rw = new RedisWrapper();
            rw.Connect(TestParameters.RedisHostWithPorts, TestParameters.RedisAuth, false, TestParameters.RedisClusterMode, TestParameters.RedisDatabase);

        }


        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        public static void FinishRedisTesting() {

            Logger.Log("Disconnecting from the Redis database");
            rw.Disconnect();

        }



        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>
        ///     Random tests - returns a set of IDs
        /// </summary>
        public static List<uint> RedisPerformanceComparisonCustom() {

            bool success = false;

            // Pause to enable GC
            GC.Collect();
            Thread.Sleep(3000);

            Logger.LogSubHeading("Starting Redis performance comparison of CUSTOM code ...");

            HashSet<uint> objIDs = new HashSet<uint>();
            int i = 0;
                        

            string objectName = TestObj.ObjectName; // "RedisTestObj";
            List<DatabaseIndexInfo> riis = rw.IndexFindAll(new TestObj());

            //-----2----- Test RedisWrapper
            {
                Logger.Log("Now testing the RedisWrapper...");
                DateTime RVTest = DateTime.Now;
                List<Task<List<uint>>> rvTasks2 = new List<Task<List<uint>>>();
                //List<Task<List<uint>>> rvTasks3 = new List<Task<List<uint>>>();

                // Firing off the search requests ...
                Logger.Log("Firing off the search requests ...");
                int chunkSize = 10000;
                if (chunkSize < 10) {
                    chunkSize = 10;
                } else if (chunkSize > TestParameters.PatternsToSearchFor.Count) {
                    chunkSize = TestParameters.PatternsToSearchFor.Count;
                }
                int startIndex = 0;


                {
                    rvTasks2 = new List<Task<List<uint>>>();
                    for (i = startIndex; i < startIndex + chunkSize; i++) {
                        //rvTasks2.Add(rw.SearchObjectsStart(objectName, TestParameters.PatternsToSearchFor[i], true));
                        rvTasks2.Add(rw.SearchObjects(objectName, TestParameters.PatternsToSearchFor[i], true));

                        RedisWrapper.DebugStart(rvTasks2[rvTasks2.Count - 1].Id, TestParameters.PatternsToSearchFor[i], riis);

                        Logger.Log(i, 100, TestParameters.NumSearchPatterns);
                    }
                    Logger.Log(i, 1, TestParameters.NumSearchPatterns);

                    //                    List<List<uint>> results = rw.AwaitResultsSeparately(rvTasks2);

                    ///////////////////////////////////////////////// Might need to minimise the memory here ...
                    //rvTasks3 = new List<Task<List<uint>>>();
                    //for (i = startIndex; i < startIndex + chunkSize; i++) {
                    //                        rvTasks3.Add(rw.SearchObjectsFinish(objectName, TestParameters.PatternsToSearchFor[i], results[i], true));
                    //                        // NEEED TO UPDATE THE DEBUG HERE
                    //                        Logger.Log(i, 100, TestParameters.NumSearchPatterns);
                    //                    }
                    //                    Logger.Log(i, 1, TestParameters.NumSearchPatterns);

                    //objIDs.UnionWith(rw.AwaitResults(rvTasks3));
                    objIDs.UnionWith(rw.AwaitResults(rvTasks2));
                }

                while (i < TestParameters.NumSearchPatterns) {
                    while (i % chunkSize != 0) {
                        Thread.Sleep(10);
                    }
                    startIndex += chunkSize;
                    {
                        //Logger.Log("\nExtracting chunk starting " + startIndex + " and length " + chunkSize + " ...");
                        rvTasks2 = new List<Task<List<uint>>>();
                        for (i = startIndex; i < startIndex + chunkSize; i++) {
                            //rvTasks2.Add(rw.SearchObjectsStart(objectName, TestParameters.PatternsToSearchFor[i], true));
                            rvTasks2.Add(rw.SearchObjects(objectName, TestParameters.PatternsToSearchFor[i], true));

                            RedisWrapper.DebugStart(rvTasks2[rvTasks2.Count - 1].Id, TestParameters.PatternsToSearchFor[i], riis);

                            Logger.Log(i, 100, TestParameters.NumSearchPatterns);
                        }
                        Logger.Log(i, 1, TestParameters.NumSearchPatterns);

                        //List<List<uint>> results = rw.AwaitResultsSeparately(rvTasks2);

                        ///////////////////////////////////////////////// Might need to minimise the memory here ...
                        //rvTasks3 = new List<Task<List<uint>>>();
                        //for (i = startIndex; i < startIndex + chunkSize; i++) {
                            //rvTasks3.Add(rw.SearchObjectsFinish(objectName, TestParameters.PatternsToSearchFor[i], results[i], true));
                            // NEEED TO UPDATE THE DEBUG HERE
                            //Logger.Log(i, 100, TestParameters.NumSearchPatterns);
                        //}
                        //Logger.Log(i, 1, TestParameters.NumSearchPatterns);

                        //objIDs.UnionWith(rw.AwaitResults(rvTasks3));
                        objIDs.UnionWith(rw.AwaitResults(rvTasks2));
                    }
                }

                // and now here lets sort the Debug results and have a look at the outcome........
                if (RedisWrapper.DoDebug == true) {
                    RedisWrapper.Debug.Sort(DatabaseDebug.Sort("Duration", true));
                    //RedisWrapper.Debug.Add(new RedisDebug(rvTasks2[rvTasks2.Count - 1].Id, DateTime.Now, TestParameters.PatternsToSearchFor[i]));

                    foreach (DatabaseDebug rb in RedisWrapper.Debug) {
                        if (rb.IsFaulted == true) {
                            Logger.LogWarning(rb.SearchPatternsAsText + " and took " + rb.Duration + "ms. ");
                        } else {
                            Logger.Log(rb.SearchPatternsAsText + " and took " + rb.Duration + "ms. ");
                        }
                    }

                }


                TimeSpan RVTested2 = DateTime.Now.Subtract(RVTest);
                Logger.Log("");
                Logger.Log("Total num objects = " + objIDs.Count.ToString("N0") + ".  And number of faults observed = XXXXXXXXXX.");
                Logger.Log("Time to process:" + RVTested2.TotalSeconds);

            }

            // Pause to enable GC
            Logger.Log("Pausing to enable garbage collection ...");
            GC.Collect();
            Thread.Sleep(3000);


            return objIDs.ToList();
        }


        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>
        ///     Random tests - returns a set of IDs
        /// </summary>
        public static List<uint> RedisPerformanceComparisonRaw() {

            bool success = false;

            // Pause to enable GC
            GC.Collect();
            Thread.Sleep(3000);

            Logger.LogSubHeading("Starting Redis performance comparison - RAW StackExchange.Redis queries ...");

            HashSet<uint> objIDs = new HashSet<uint>();
            int i = 0;


            string objectName = TestObj.ObjectName; // "RedisTestObj";

            //-----1----- Test Raw Redis
            {
                Logger.Log("Testing raw Redis queries ...");

                Task<bool> keyExistsT = rw.DB.KeyExistsAsync(objectName + ":1");
                bool keyExists = keyExistsT.Result;

                Logger.Log("Firing off the queries ...");
                DateTime RVTest = DateTime.Now;
                List<Task<RedisValue[]>> rvTasks = new List<Task<RedisValue[]>>();

                for (i = 0; i < TestParameters.NumSearchPatterns; i++) {
                    // cycle through the connections and build the list of async tasks ...
                    DatabaseSearchPattern rsp = TestParameters.PatternsToSearchFor[i][0];
                    if (rsp.SearchType == SearchType.PrimaryKey) {
                        // PK Query
                        //if (rw.DB.KeyExists("RedisTestObj:PrimaryKey", rsp.Score)) {
                        if (rw.DB.KeyExists(rw.KeyName(objectName, rw.ConvertID(rsp.Score)))) {
                            objIDs.Add(rw.ConvertID(rsp.Score));
                        }
                    } else {
                        // Range Query
                        //rvTasks.Add(rw.DB.SortedSetRangeByScoreAsync("{"+ objectName + ":Index:TestInt}", rsp.ScoreMin, rsp.ScoreMax));
                        rvTasks.Add(rw.DB.SortedSetRangeByScoreAsync("{" + objectName + ":i:" + rsp.Parameter + "}", rsp.ScoreMin, rsp.ScoreMax));
                    }

                    Logger.Log(i, 100, TestParameters.NumSearchPatterns);
                }
                Logger.Log(i, 1, TestParameters.NumSearchPatterns);

                // Cycle through the tasks and skip over them (do not block) if they have not yet completed...
                Logger.Log("");
                Logger.Log("Now awaiting the results");
                if (objIDs.Count == 0) {
                    objIDs.UnionWith(rw.AwaitResults(rvTasks));
                }

                TimeSpan RVTested = DateTime.Now.Subtract(RVTest);
                Logger.Log("");
                Logger.Log("Total num objects = " + objIDs.Count.ToString("N0") + ".");
                Logger.Log("Time to process:" + RVTested.TotalSeconds);

            }






            // Pause to enable GC
            Logger.Log("Pausing to enable garbage collection ...");
            GC.Collect();
            Thread.Sleep(3000);


            return objIDs.ToList();
        }



        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>
        ///     Random tests - returns a set of IDs
        /// </summary>
        public static void RedisPerformanceObjectExtractionComparison(List<uint> objIDs) {

            Logger.Log("Here we go - RedisPerformanceObjectExtractionComparison ...");
            // OK now we dont want to kill stuff!!!
            // if the num of objIDs is more than 1 meeeeeelllllion, then lets truncate it ...
            if ( objIDs.Count > TestParameters.MaxNumObjects) {
                Logger.LogWarning("RedisPerformanceObjectExtractionComparison - " + objIDs.Count
                    + " is too many objects to extract in this test; the  maximum number of objects recommended is " + TestParameters.MaxNumObjects + ", so defaulting to the first n objects ...");

                //objIDs = objIDs.CopyTo(.GetRange(0, 1000000);
                objIDs = objIDs.GetRange(0, TestParameters.MaxNumObjects);
            }


            //-----a----- Test RedisWrapper
            Logger.LogSubHeading("Now testing the RedisWrapper for extracting objects using reflection...");
            Logger.Log("Attempting to extract "+objIDs.Count+" objects ...");
            //RedisWrapper rw = new RedisWrapper();
            //rw.Connect(TestParameters.RedisHostWithPorts, TestParameters.RedisAuth, false, TestParameters.RedisDatabase);

            // And now test getting the objects using async reflection ....
            
            DateTime RVTest3 = DateTime.Now;
            List<TestObj> output = new List<TestObj>();

            Logger.Log("Starting the object extraction tasks ...");
            List<Task<HashEntry[]>> tasks = rw.GetObjectsStart(new TestObj(), objIDs);

            Logger.Log("Finishing the object extraction tasks ...");
            //Task<List<object>> tObj = rw.GetObjectsFinish(new TestObj(), tObjs);
            List<object> tObjs = rw.GetObjectsFinish(new TestObj(), tasks);

            Logger.Log("And now casting the objects ...");
            output = TestObj.ParseList(tObjs);

            
//            foreach (object o in tObj.Result) {
//                objReflection.Add(o);
//            }
            TimeSpan RVTested3 = DateTime.Now.Subtract(RVTest3);
            Logger.Log("Time to process:" + RVTested3.TotalSeconds + " and " + output.Count.ToString("N0") + " objects generated. ");


            // Pause to enable GC
            Logger.Log("Pausing to enable garbage collection ...");
            output = null;
            tObjs = null;
            tasks = null;
            GC.Collect();
            Thread.Sleep(3000);


            // And now test getting the objects using the RedisTestObj direct code ....
            Logger.LogSubHeading("Now testing using the specific RedisTestObjs ...");
            Logger.Log("Attempting to extract " + objIDs.Count + " objects ...");
            DateTime RVTest4 = DateTime.Now;
            List<TestObj> rtoObjs = new List<TestObj>();

            List<Task<HashEntry[]>> tObjs2 = new List<Task<HashEntry[]>>();

            Logger.Log("Starting the object extraction tasks ...");
            foreach (uint id in objIDs) {
                tObjs2.Add(rw.GetObjectStart(id));
            }

            Logger.Log("Finishing the object extraction tasks ...");
            foreach (Task<HashEntry[]> the in tObjs2) {
                rtoObjs.Add(rw.GetObjectFinish(the));
            }

            TimeSpan RVTested4 = DateTime.Now.Subtract(RVTest4);
            Logger.Log("Time to process:" + RVTested4.TotalSeconds + " and "+rtoObjs.Count.ToString("N0")+" objects generated. ");


            //rw.Disconnect();

                               




            //string ended = "";

        }






        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>
        ///     Writes the data contained in the list of RedisTestObj objects in the TestParameters.RandomObjects property.
        ///     Use the TestParameters.GenerateRandomObjects method to populate the RandomObjects property.
        /// </summary>
        public static void PerformanceTestRedisWrite() {
            bool success = false;

            Logger.LogSubHeading("Testing Redis data writing performance");

            //-----a----- Get the object properties for our test object
            TestObj rto = new TestObj();
            string objName = null;
            PropertyInfo[] objPI = null;
            rw.GetObjectProperties(rto, out objName, out objPI);

            //-----b---- Clear out any existing data ..
            if (TestParameters.DoWrite == true) {
                Logger.Log("Clearing any existing data...");
                success = rw.ClearData(TestObj.ObjectName);
               
            }

            // Write the data to the db and index the required fields
            Logger.Log("Writing data");
            DateTime writeStart = DateTime.Now;

            if (TestParameters.DoWrite == true) {

//                PropertyInfo[] pi = rto.GetType().GetProperties();

                //  With a clustered database, we are better off indexing the data first as with most of the indexes, they will need to be clustered on one specific db
                // And index a few of the fields...
                // This means that the actual objects are then concentrated on the remaining cluster nodes...
                Logger.LogSubHeading("Extracting TestInt ...");
                List<KeyValuePair<uint, double>> data = TestObj.BuildNumericList("TestInt", objPI, TestParameters.RandomObjects);
                Logger.Log("Indexing TestInt ...");
                success = success & rw.IndexNumericProperty(TestObj.ObjectName, "TestInt", rto.TestInt.GetType().Name, data);

                GC.Collect();

                Logger.LogSubHeading("Extracting TestLong ...");
                data = TestObj.BuildNumericList("TestLong", objPI, TestParameters.RandomObjects);
                Logger.Log("Indexing TestLong ...");
                success = success & rw.IndexNumericProperty(TestObj.ObjectName, "TestLong", rto.TestLong.GetType().Name, data);

                GC.Collect();

                Logger.LogSubHeading("Extracting TestDouble ...");
                data = TestObj.BuildNumericList("TestDouble", objPI, TestParameters.RandomObjects);
                Logger.Log("Indexing TestDouble ...");
                success = success & rw.IndexNumericProperty(TestObj.ObjectName, "TestDouble", rto.TestDouble.GetType().Name, data);

                GC.Collect();

                // 19-Aug-2016 - there is little merit in extracting bools into indexes in Redis as it's 50:50 - better to just scan the objects!
                //                Logger.Log("Extracting TestBool ...");
                //                data = RedisTestObj.BuildNumericList("TestBool", objPI, TestParameters.RandomObjects);
                //                Logger.Log("Indexing TestBool ...");
                //                success = success & rw.IndexBool(RedisTestObj.ObjectName, "TestBool", data);

                Logger.LogSubHeading("Extracting TestDT ...");
                data = TestObj.BuildNumericList("TestDT", objPI, TestParameters.RandomObjects);
                Logger.Log("Indexing TestDT ...");
                success = success & rw.IndexDateTime(TestObj.ObjectName, "TestDT", data);

                data = null;
                GC.Collect();

                Logger.LogSubHeading("Extracting TestStr ...");
                List<KeyValuePair<uint, string>> data2 = TestObj.BuildStringList("TestStr", objPI, TestParameters.RandomObjects);
                Logger.Log("Indexing TestStr ...");
                success = success & rw.IndexString(TestObj.ObjectName, "TestStr", data2);

                data2 = null;
                GC.Collect();

                // So lets write this to the database
                Logger.LogSubHeading("Indexing completed - now starting to writing the data...");
                success = rw.WriteDataAsHash(TestParameters.RandomObjects);
                Logger.Log("Writing data completed");
            }

            Logger.Log("Finished writing and indexing");
            TestParameters.WriteRedis = DateTime.Now.Subtract(writeStart);

            // 15-Aug-2016 - And lastly - lets make sure we can get all the indexes!!!
            List<DatabaseIndexInfo> riis = rw.IndexFindAll(rto);
            foreach( DatabaseIndexInfo rii in riis) {
                Logger.Log("Found index for property "+rii.IndexName+" of type "+rii.IndexType
                    +" and search type "+rii.SearchType.ToString()
                    +" with min of "+ rii.MinMax.Key 
                    + " and max of " + rii.MinMax.Value);
            }

            string ohFuck = "";
        }

        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>
        /// 
        /// </summary>
        public static void PerformanceTestRedisRead() {

            Logger.LogSubHeading("Testing Redis threaded reading!");
            DateTime RedisThreadedStartTime = DateTime.Now;
            //bool success = false;
            NumCompleted = 0;

            // Now lets load the numberic indices into virtual memory
            //if (RedisWrapper.LoadIndicesIntoVM == true) {
            //    Logger.Log("Loading the indices into VM...");
            //    success = rw.LoadIndices("RedisTestObj", new List<string>() { "TestInt", "TestLong", "TestDouble" });
            //}

            Logger.Log("Now starting to test the patterns");


            // 15-Aug-2016 - And lastly - lets make sure we can get all the indexes!!!
            RedisWrapper.IndexInfo = rw.IndexFindAll(new TestObj());


            /*
             *      2-Aug-2016 - OK, we need to be cleverer with the chunk sizes.  Redis is ONLY faster than MySQL when it 100% performs in RAM.
             *      As soon as the data size of the Redis database and the extraction code spills over our RAM limits and starts caching, it dramatically slows down.
             *      So when building our pipeline of queries we need to have an idea of how much data we are potentially throwing into memory.
             
             *      The problem comes when trying to parcel together lots of Redis queries to speed these up and utlise the pipeline.  This means that instead
             *      of just one query pulling out e.g. IDs, we have all of the queries trying to load their results into memory at the same time!!
             *      To give an example - if we have 1,000 queries and a database with 1,000,000 objects in it
             *      
             *      Long is –9,223,372,036,854,775,808 to 9,223,372,036,854,775,807
             *      uLong is 0 to 18,446,744,073,709,551,615
            */

            // Good to chunk this up!!
            int chunkSize = DNThreadManager.CalculateOptimalChunkSize(TestParameters.NumSearchPatterns);
            chunkSize = (chunkSize > 1000) ? 1000 : chunkSize;

            // HACK IT!!!
            chunkSize = 1000; // Good at 100000
            //chunkSize = 100;
            //chunkSize = 500; // good at 1m
            //chunkSize = 400;
            //chunkSize = 350;

            // And catch piddly tests ...
            if ( chunkSize > TestParameters.NumSearchPatterns) {
                chunkSize = TestParameters.NumSearchPatterns;
            }

            

            // disable threading for now on this option!!!
            //if (TestParameters.UseThreading == false) {               
            int startIndex = 0;
            NumCompleted = 0;
            IDs = new HashSet<uint>();

            //PerformanceTestRedisReadThreadBlock(TestParameters.NumSearchPatterns, 0);
            PerformanceTestRedisSearchThreadBlock(chunkSize, startIndex);

            while (NumCompleted < TestParameters.NumSearchPatterns) {
                startIndex += chunkSize;
                PerformanceTestRedisSearchThreadBlock(chunkSize, startIndex);
            }

            


            //} else {
            //bool success = DNThreadManager.Run("RedisRead", NumIterations, 2500, new Action<int, int>( PerformanceTestRedisRead));
            // and lets also make sure these chunks are a sensible size too
            //success = DNThreadManager.Run("RedisRead", TestParameters.NumSearchPatterns, chunkSize, PerformanceTestRedisSearchThreadBlock);
            //            }

            Logger.Log("Found " + IDs.Count + " objects matching one or more of the search patterns ...");
            TestParameters.ReadRedisThreaded = DateTime.Now.Subtract(RedisThreadedStartTime);


            // Then get the objects and note that we don't count this in the timings .......
            PerformanceTestRedisGetObjects();


            Logger.Log("Time to process:" + TestParameters.ReadRedisThreaded.TotalSeconds + ". ");
            Logger.Log("Threaded testing completed");

        }


        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>
        /// 
        /// </summary>
        /// <param name="chunkSize"></param>
        /// <param name="startIndex"></param>
        public static void PerformanceTestRedisSearchThreadBlock(int chunkSize, int startIndex) {

            TestObj rto = new TestObj();

            int i = startIndex;
            if ((startIndex + chunkSize) >= TestParameters.PatternsToSearchFor.Count) {
                chunkSize = TestParameters.PatternsToSearchFor.Count - startIndex;
            }

            Logger.Log("Starting chunk with index " + startIndex + " and size " + chunkSize + " ...");

            //-----a----- Fire off the search queries
            List<Task<List<uint>>> tasks = new List<Task<List<uint>>>();
            //List<Task<List<uint>>> tasksS = new List<Task<List<uint>>>();
            //List<Task<List<uint>>> tasksF = new List<Task<List<uint>>>();


            for (i = startIndex; i < (startIndex + chunkSize); i++) {
                List<DatabaseSearchPattern> rsps = TestParameters.PatternsToSearchFor[i];
                //tasksS.Add(rw.SearchObjectsStart(RedisTestObj.ObjectName, rsps, true));
                tasks.Add(rw.SearchObjects(TestObj.ObjectName, rsps, true));

                //RedisWrapper.DebugStart(tasksS[tasksS.Count - 1].Id, rsps, RedisWrapper.IndexInfo);
                RedisWrapper.DebugStart(tasks[tasks.Count - 1].Id, rsps, RedisWrapper.IndexInfo);

                Logger.Log(i - startIndex, 100, chunkSize);
            }
            Logger.Log(i - startIndex, 100, chunkSize);

            //Thread.Sleep(1000);

            //List<List<uint>> results = rw.AwaitResultsSeparately(tasksS);

            //Logger.Log("\nAnd now finishing the searching of these objects ...");            
            ///////////////////////////////////////////////// Might need to minimise the memory here ...
            //for (i = startIndex; i < startIndex + chunkSize; i++) {
            // tasksF.Add(rw.SearchObjectsFinish(RedisTestObj.ObjectName, TestParameters.PatternsToSearchFor[i], results[i], true));
            // NEEED TO UPDATE THE DEBUG HERE
            //Logger.Log(i, 100, TestParameters.NumSearchPatterns);
            //}
            //Logger.Log(i, 1, TestParameters.NumSearchPatterns);


            //-----b----- Now lets wait for our array of IDs
            // Now do the union...

            //List<uint> allIDs = rw.AwaitResults(tasksF);
            int previousCount = IDs.Count;
            lock(IDs) {
                //IDs.UnionWith(allIDs);
                IDs.UnionWith(rw.AwaitResults(tasks));                
            }


            //HashSet<long> allIDs = RedisWrapper.AwaitResults(tasks).Result;
            //HashSet<long> allIDs = RedisWrapper.AwaitResults(tasks);
            //Logger.Log("Found "+allIDs.Count+" ids matching our search patterns... ");

            // Note that this will now be cumulative ..
            Logger.Log("Found " + (IDs.Count-previousCount) + " NEW ids matching our search patterns (and "+IDs.Count+" now in total)... ");


            // Do the printing of all the specific queries only if DoDebug is set to true ....
            if (RedisWrapper.DoDebug == true) {
                 RedisWrapper.Debug.Sort(DatabaseDebug.Sort("Duration", true));

                int counter = 0;
                foreach (DatabaseDebug rb in RedisWrapper.Debug) {
                    if ( rb.IsFaulted == true) {
                        Logger.LogWarning(rb.SearchPatternsAsText + " and took " + rb.Duration + "ms. ");
                    } else {
                        Logger.Log("Search " + (++counter) + " " + rb.SearchPatternsAsText + " and took " + rb.Duration + "ms. ");
                    }                   
                }
            }


            //HashSet<long> ids = null;
            //if ( allIDs.Count > TestParameters.MaxNumObjects) {
            //    foreach( long id in allIDs) {
            //        ids.Add(id);
            //        if (ids.Count >= TestParameters.MaxNumObjects) {
            //            break;
            //        }
            //    }
            //    allIDs = null;
            //} else {
            //    ids = allIDs;
            //}



            //-----d-----
//            if (TestParameters.UseThreading == true) {
//                DNThreadManager.IncrementNumIterationsCompleted();
//            } else {
//                Logger.Log(i, 100, chunkSize);
//            }

            NumCompleted += chunkSize;


            //            if (TestParameters.UseThreading == true) {
            //                DNThreadManager.IncrementNumIterationsCompleted(chunkSize);
            //            } else {

            //            }

            Logger.Log("");
//            Logger.Log(log.ToString());

        }

        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        public static void PerformanceTestRedisGetObjects() {

            List<uint> tempIDs = new List<uint>();

            if (IDs.Count > TestParameters.MaxNumObjects) {
                Logger.LogWarning("PerformanceTestRedisGetObjects - " + IDs.Count 
                    + " is too many objects to extract in this test; the  maximum number of objects recommended is "+ TestParameters.MaxNumObjects + ", so defaulting to the first n objects ...");
                uint[] tempArr = new uint[TestParameters.MaxNumObjects];
                IDs.CopyTo(tempArr, 0, TestParameters.MaxNumObjects);
                tempIDs.AddRange(tempArr);
            }


            Logger.Log("Starting to get "+ tempIDs.Count+" objects from Redis ...");

            TestObj rto = new TestObj();

            //-----c-----
            Logger.Log("Starting to get our objects...");
            List<Task<HashEntry[]>> tasksGetObj = rw.GetObjectsStart(rto, tempIDs);

            Logger.Log("Completing the extraction of our objects...");
            //Task<List<object>> objTasks = rw.GetObjectsFinish(rto, tasksGetObj);

            List<object> tObjs = rw.GetObjectsFinish(rto, tasksGetObj);

            Logger.Log("And now casting the objects ...");
            List<TestObj> rtos = new List<TestObj>();
            rtos = TestObj.ParseList(tObjs);
            
//            rtos = TestObj.ParseList(objTasks.Result);
            Logger.Log("Extracted " + rtos.Count + " objects.");

            // clean up the static variables to minimise memory usage
            IDs = new HashSet<uint>();

        }


        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        public static void PerformanceTestRedisCSharp() {
            bool success = false;

            Logger.LogSubHeading("Testing Redis with C Sharp checks!");
            Logger.Log("Reading data (including loading all the object into memory in the first place!)");
            DateTime readStart = DateTime.Now;

            // Read the data from the DB ...           
            Logger.Log("Getting all the objects...");
            List<object> objs = rw.GetAllObjects(new TestObj());
            TestParameters.AllObjects = TestObj.ParseList(objs);
            Logger.Log("Extracted " + TestParameters.AllObjects.Count + " objects...");

            IDs = new HashSet<uint>();

            // And now do the pattern matching - no need to set a specific chunk size for these ones ...
            Logger.Log("Starting threaded processing ...");
            if (TestParameters.UseThreading == true) {
                success = DNThreadManager.Run("RedisCSharp", TestParameters.PatternsToSearchFor.Count, PerformanceTestRedisCSharpThreadBlock);
            } else {
                PerformanceTestRedisCSharpThreadBlock(TestParameters.PatternsToSearchFor.Count, 0);
            }

            Logger.Log("Found " + IDs.Count + " objects matching one or more of the search patterns ...");

            TestParameters.ReadRedisCSharp = DateTime.Now.Subtract(readStart);
            Logger.Log("Redis with CSharp tests completed");

            // clean up the static variables to minimise memory usage
            IDs = new HashSet<uint>();

        }

        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>
        ///  This is fast because it is non-blocking ....!
        /// </summary>
        /// <param name="chunkSize"></param>
        /// <param name="startIndex"></param>
        public static void PerformanceTestRedisCSharpThreadBlock(int chunkSize, int startIndex) {

            Logger.Log("Starting chunk with index " + startIndex + " and size " + chunkSize + " ...");

            StringBuilder log = new StringBuilder();
            StringBuilder queryTxt = new StringBuilder();

            int searchIndex = startIndex;

            for (int i = startIndex; i < (startIndex + chunkSize); i++) {
                List<DatabaseSearchPattern> rsps = TestParameters.PatternsToSearchFor[i];

                // Scan for the objects ...
                List<TestObj> rtos = rw.ScanObjects(TestObj.ObjectName, TestParameters.AllObjects, rsps, true);

                // Now append the ids to the global list ...
                foreach(TestObj rto in rtos) {
                    if ( IDs.Contains(rw.ConvertID(rto.ID)) == false) {
                        lock( IDs) {
                            IDs.Add(rw.ConvertID(rto.ID));
                        }                        
                    }
                }


                // Do the printing of all the specific queries only if DoDebug is set to true ....
                if (RedisWrapper.DoDebug == true) {
                    queryTxt.Clear();
                    foreach (DatabaseSearchPattern rsp in rsps) {
                        queryTxt.Append(" " + rsp.AsText());
                    }

                    log.Append("\n" + "Search " + i + " found " + rtos.Count + " objects. Query Text: " + queryTxt);
                }

//                log.Append("\n" + "Search " + (searchIndex++) + " found " + rtos.Count + " objects. Query Text: " + queryTxt);
                if (TestParameters.UseThreading == true) {
                    DNThreadManager.IncrementNumIterationsCompleted();
                } else {
                    Logger.Log(i, 100, chunkSize);
                }
            }

            Logger.Log("");
            Logger.Log(log.ToString());

        }



        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>
        ///     Generates a series of simple integer range searches... useful for more specific testing
        /// </summary>
        public static void TestSorting() {

            List<DatabaseIndexInfo> riis = rw.IndexFindAll(new TestObj());

            int randomVal = MGLEncryption.GenerateRandomInt(1, 1000000);
            long randomVal2 = MGLEncryption.GenerateRandomLong(8000000000, 12000000000);
            double randomVal3 = MGLEncryption.GenerateRandomDouble(0, 1);
            DateTime randomVal4 = MGLEncryption.GenerateRandomDateTime(TestParameters.DateMin, TestParameters.DateMax);

            List<DatabaseSearchPattern> testPatterns = new List<DatabaseSearchPattern>() {

                // Bool
                new DatabaseSearchPattern("TestBool", MGLEncryption.GenerateRandomBool()),
                // Score - equivalent (int)
                new DatabaseSearchPattern("TestInt", randomVal),
                // Score - greater than
                new DatabaseSearchPattern("TestInt", randomVal, true),
                // Score - less than
                new DatabaseSearchPattern("TestInt", randomVal, false),
                // Score - between
                new DatabaseSearchPattern("TestInt", randomVal, randomVal + TestParameters.SearchRangeSize),
                // Text - Starts with
                new DatabaseSearchPattern("TestStr", MGLEncryption.GetSalt(RedisWrapper.TextIndexLength).ToString().ToLower()),
                // Score - between (long)
                new DatabaseSearchPattern("TestLong", randomVal2, randomVal2 + (4000000000*TestParameters.SearchRangeSize/1000000)),
                // Primary key
                new DatabaseSearchPattern((long)randomVal),
                // Score - between
                new DatabaseSearchPattern("TestDouble", randomVal3, randomVal3 + ((double)TestParameters.SearchRangeSize/1000000.0)),
                // Date time - betwen
                new DatabaseSearchPattern("TestDT", randomVal4, randomVal4.AddDays(1)),
                // Date time - less than
                new DatabaseSearchPattern("TestDT", randomVal4, false),
                // Date time - greater than
                new DatabaseSearchPattern("TestDT", randomVal4, true),
                // Date time - equivalent
                new DatabaseSearchPattern("TestDT", randomVal4)


            };


            // sort it
            testPatterns.Sort(DatabaseSearchPattern.Sort(riis));

            string isItGucci = "";

        }




    }
}
