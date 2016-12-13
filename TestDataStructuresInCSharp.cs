using MGL.Data.DataUtilities;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

//--------------------------------------------------------------------------------------------------------------------------------------------------------------------
namespace DataNirvana.Database {

    //-----------------------------------------------------------------------------------------------------------------------------------------------------------------
    /// <summary>
    ///     Compares the performance of Lists, HashSets and Dictionaries
    /// </summary>
    public class TestDataStructuresInCSharp {

        public static int NumObjects = 1000;

        public static int TimeOutSeconds = 120; // Two minutes ....

        //-----0----- Timings ...
        static TimeSpan tsListWrite;
        static TimeSpan tsListRead;
        static TimeSpan tsListUse1;
        static TimeSpan tsListUse2;

        static TimeSpan tsSortedListWrite;
        static TimeSpan tsSortedListRead;
        static TimeSpan tsSortedListUse1;
        static TimeSpan tsSortedListUse2;

        static TimeSpan tsDictionaryWrite;
        static TimeSpan tsDictionaryRead;
        static TimeSpan tsDictionaryUse1;
        static TimeSpan tsDictionaryUse2;

        static TimeSpan tsHashSetWrite;
        static TimeSpan tsHashSetRead;
        static TimeSpan tsHashSetUse1;
        static TimeSpan tsHashSetUse2;

        static TimeSpan tsHashSetListWrite;
        static TimeSpan tsHashSetListUse;


        //-----0-----
        static List<object> data1;
        static List<object> data2;
        static List<object> data3;


        //-----a-----
        static List<ulong> list1;
        static List<ulong> list2;
        static List<ulong> list3;

        //-----b-----
        static SortedList<ulong, long> sList1;
        static SortedList<ulong, long> sList2;
        static SortedList<ulong, long> sList3;

        //-----c-----
        static Dictionary<ulong, long> dict1;
        static Dictionary<ulong, long> dict2;
        static Dictionary<ulong, long> dict3;

        //-----d-----
        static HashSet<ulong> set1;
        static HashSet<ulong> set2;
        static HashSet<ulong> set3;


        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        public static void Results() {

            Logger.LogSubHeading("Test Data Structures - and the winner is ....");

            Logger.Log("List - Write - \t"+Math.Round(tsListWrite.TotalMilliseconds,2)+".");
            Logger.Log("List - Read - \t" + Math.Round(tsListRead.TotalMilliseconds, 2) + ".");
            Logger.Log("List - Use (Contains) - \t" + Math.Round(tsListUse1.TotalMilliseconds, 2) + ".");
            Logger.Log("List - Use (Membership) - \t" + Math.Round(tsListUse2.TotalMilliseconds, 2) + ".");
            Logger.Log("");

            Logger.Log("SortedList - Write - \t" + Math.Round(tsSortedListWrite.TotalMilliseconds, 2) + ".");
            Logger.Log("SortedList - Read - \t" + Math.Round(tsSortedListRead.TotalMilliseconds, 2) + ".");
            Logger.Log("SortedList - Use (Contains) - \t" + Math.Round(tsSortedListUse1.TotalMilliseconds, 2) + ".");
            Logger.Log("SortedList - Use (Membership) - \t" + Math.Round(tsSortedListUse2.TotalMilliseconds, 2) + ".");
            Logger.Log("");

            Logger.Log("HashSet - Write - \t" + Math.Round(tsHashSetWrite.TotalMilliseconds, 2) + ".");
            Logger.Log("HashSet - Read - \t" + Math.Round(tsHashSetRead.TotalMilliseconds, 2) + ".");
            Logger.Log("HashSet - Use (Contains) - \t" + Math.Round(tsHashSetUse1.TotalMilliseconds, 2) + ".");
            Logger.Log("HashSet - Use (Membership) - \t" + Math.Round(tsHashSetUse2.TotalMilliseconds, 2) + ".");
            Logger.Log("");

            Logger.Log("Dictionary - Write - \t" + Math.Round(tsDictionaryWrite.TotalMilliseconds, 2) + ".");
            Logger.Log("Dictionary - Read - \t" + Math.Round(tsDictionaryRead.TotalMilliseconds, 2) + ".");
            Logger.Log("Dictionary - Use (Contains) - \t" + Math.Round(tsDictionaryUse1.TotalMilliseconds, 2) + ".");
            Logger.Log("Dictionary - Use (Membership) - \t" + Math.Round(tsDictionaryUse2.TotalMilliseconds, 2) + ".");
            Logger.Log("");

            Logger.Log("HashSet with List - Write - \t" + Math.Round(tsHashSetListWrite.TotalMilliseconds, 2) + ".");
            Logger.Log("HashSet with List - Use (Membership) - \t" + Math.Round(tsHashSetListUse.TotalMilliseconds, 2) + ".");
            Logger.Log("");

        }


        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>
        ///     Generates the random data that is consumed by the specific data structure test methods below.
        /// </summary>
        public static void GenerateData() {

            Logger.LogSubHeading("Test Data Structures - Generating data ....");

            Logger.Log("Setting the number of iterations to perform in the TestParameters.NumIterations property ....");
            int currentNumIterations = TestParameters.NumIterations;
            bool currentDoWrite = TestParameters.DoWrite;

            TestParameters.DoWrite = true;
            TestParameters.NumIterations = TestDataStructuresInCSharp.NumObjects;
            Logger.Log("Generating three sets of "+ TestDataStructuresInCSharp.NumObjects + " objects ....");

            TestParameters.GenerateRandomObjects();
            data1 = new List<object>();
            foreach( object tObj in TestParameters.RandomObjects) {
                data1.Add(tObj);
            }

            TestParameters.GenerateRandomObjects();
            data2 = new List<object>();
            foreach (object tObj in TestParameters.RandomObjects) {
                data2.Add(tObj);
            }

            TestParameters.GenerateRandomObjects();
            data3 = new List<object>();
            foreach (object tObj in TestParameters.RandomObjects) {
                data3.Add(tObj);
            }

            // lastly, lets be nice and reset the test parameters num iterations property
            TestParameters.NumIterations = currentNumIterations;
            TestParameters.DoWrite = currentDoWrite;
            Thread.Sleep(1000);
            Logger.Log("Finished generating the random data ....");

        }


        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>
        ///     Test the reading, writing and using of List data structures
        /// </summary>
        public static void TestLists() {
            Logger.LogSubHeading("Testing Lists ...");

            Thread.Sleep(1000);

            //-----WRITE-----
            Logger.Log("Writing List data...");
            DateTime start = DateTime.Now;

            long totalMB4 = GC.GetTotalMemory(true);
            list1 = new List<ulong>();
            foreach( TestObj rto in data1) {
                list1.Add(rto.ID);
            }
            long totalMAfter = GC.GetTotalMemory(true);
            long diff = totalMAfter - totalMB4;

            list2 = new List<ulong>();
            foreach (TestObj rto in data2) {
                list2.Add(rto.ID);
            }
            list3 = new List<ulong>();
            foreach (TestObj rto in data3) {
                list3.Add(rto.ID);
            }

            Logger.Log("Wrote three structures containing " + list1.Count.ToString("N0") + ", " + list2.Count.ToString("N0") + " and " + list3.Count.ToString("N0") + " objects.");
            Logger.Log("One structure consumed approximately "+diff.ToString("N0") + " bytes of memory.");
            tsListWrite = new TimeSpan(DateTime.Now.Ticks).Subtract(new TimeSpan(start.Ticks));


            //-----READ-----
            Logger.Log("Reading List data...");
            start = DateTime.Now;

            long counter = 0;
            foreach (long key in list1) {
                counter += key;
            }

            Logger.Log("Read and counted all the IDs, which total " + counter.ToString("N0") + ".");
            tsListRead = new TimeSpan(DateTime.Now.Ticks).Subtract(new TimeSpan(start.Ticks));


            //-----USE-CONTAINS-----
            Logger.Log("Using List data (Contains) ...");
            start = DateTime.Now;

            List<ulong> listUse = new List<ulong>();
            foreach (ulong lo in list1) {
                TimeSpan ts = new TimeSpan(DateTime.Now.Ticks).Subtract(new TimeSpan(start.Ticks));
                if (ts.TotalSeconds >= TimeOutSeconds) {
                    Logger.Log("Quitting the reading of the list - it took too long!  Managed to complete: " + listUse.Count.ToString("N0") + " objects.");
                    break;
                }
                if (listUse.Contains(lo) == false) {
                    listUse.Add(lo);
                }
            }

            Logger.Log("First object contained " + listUse.Count.ToString("N0") + " unique values ...");
            tsListUse1 = new TimeSpan(DateTime.Now.Ticks).Subtract(new TimeSpan(start.Ticks));


            //-----USE-MEMBERSHIP-----
            Logger.Log("Using List data (Membership)...");
            start = DateTime.Now;

            // Do two big intersections
            List<ulong> list1to2 = new List<ulong>();
            foreach (ulong lo in list1.Intersect(list2)) {
                list1to2.Add(lo);
            }

            List<ulong> list1to3 = new List<ulong>();
            foreach (ulong lo in list1.Intersect(list3)) {
                list1to3.Add(lo);
            }

            // and one union ...
            List<ulong> list2to3 = new List<ulong>();
            foreach (ulong lo in list2.Union(list3)) {
                list2to3.Add(lo);
            }

            Logger.Log("Sets 1int2 contains "+ list1to2.Count.ToString("N0") + ", 1int3 contains "+ list1to3.Count.ToString("N0") + " and 2Un3 contains "+ list2to3.Count.ToString("N0") + " objects.");
            tsListUse2 = new TimeSpan(DateTime.Now.Ticks).Subtract(new TimeSpan(start.Ticks));
            // clear all the data from memory ...
            list1 = list2 = list3 = list1to2 = list1to3 = list2to3 = null;
            Thread.Sleep(1000);

        }


        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>
        ///     Test the reading, writing and using of SortedList data structures
        /// </summary>
        public static void TestSortedLists() {
            Logger.LogSubHeading("Testing SortedLists ...");

            Thread.Sleep(1000);

            //-----WRITE-----
            Logger.Log("Writing SortedList data...");
            DateTime start = DateTime.Now;

            long totalMB4 = GC.GetTotalMemory(true);
            sList1 = new SortedList<ulong, long>();
            foreach (TestObj rto in data1) {
                sList1.Add(rto.ID, rto.TestLong);
            }
            long totalMAfter = GC.GetTotalMemory(true);
            long diff = totalMAfter - totalMB4;

            sList2 = new SortedList<ulong, long>();
            foreach (TestObj rto in data2) {
                sList2.Add(rto.ID, rto.TestLong);
            }
            sList3 = new SortedList<ulong, long>();
            foreach (TestObj rto in data3) {
                sList3.Add(rto.ID, rto.TestLong);
            }

            Logger.Log("Wrote three structures containing " + sList1.Count.ToString("N0") + ", " + sList2.Count.ToString("N0") + " and " + sList3.Count.ToString("N0") + " objects.");
            Logger.Log("One structure consumed approximately " + diff.ToString("N0") + " bytes of memory.");
            tsSortedListWrite = new TimeSpan(DateTime.Now.Ticks).Subtract(new TimeSpan(start.Ticks));


            //-----READ-----
            Logger.Log("Reading SortedList data...");
            start = DateTime.Now;

            ulong counter = 0;
            foreach (KeyValuePair<ulong, long> kvp in sList1) {
                counter += kvp.Key;
            }

            Logger.Log("Read and counted all the IDs, which total " + counter.ToString("N0") + ".");
            tsSortedListRead = new TimeSpan(DateTime.Now.Ticks).Subtract(new TimeSpan(start.Ticks));


            //-----USE-CONTAINS-----
            Logger.Log("Using SortedList data (Contains) ...");
            start = DateTime.Now;

            SortedList<ulong, long> sListUse = new SortedList<ulong, long>();
            foreach (KeyValuePair<ulong, long> kvp in sList1) {
                if (sListUse.ContainsKey(kvp.Key) == false) {
                    sListUse.Add(kvp.Key, kvp.Value);
                }
            }

            Logger.Log("First object contained " + sListUse.Count.ToString("N0") + " unique values ...");
            tsSortedListUse1 = new TimeSpan(DateTime.Now.Ticks).Subtract(new TimeSpan(start.Ticks));

            //-----USE-MEMBERSHIP-----
            Logger.Log("Using SortedList data (Membership) ...");
            start = DateTime.Now;
            // Do two big intersections - Unfortunately SortedList intersections are based on the key AND the value - this is the default equity comparer!!!!
            // so we cannot use the built in intersect and union methods out of the box
            SortedList<ulong, long> sList1to2 = new SortedList<ulong, long>();
            foreach (KeyValuePair<ulong, long> kvp in sList1) {
                if (sList2.ContainsKey(kvp.Key) == true) {
                    if (sList1to2.ContainsKey(kvp.Key) == false) {
                        sList1to2.Add(kvp.Key, kvp.Value);
                    } else {
                        sList1to2[kvp.Key] = kvp.Value;
                    }
                }
            }

            SortedList<ulong, long> sList1to3 = new SortedList<ulong, long>();
            foreach (KeyValuePair<ulong, long> kvp in sList1) {
                if (sList3.ContainsKey(kvp.Key) == true) {
                    if (sList1to3.ContainsKey(kvp.Key) == false) {
                        sList1to3.Add(kvp.Key, kvp.Value);
                    } else {
                        sList1to3[kvp.Key] = kvp.Value;
                    }
                }
            }

            // and one union ... conduct by adding all of the first list, and then any from the second list that dont match
            SortedList<ulong, long> sList2to3 = new SortedList<ulong, long>();
            foreach (KeyValuePair<ulong, long> kvp in sList2) {
                sList2to3.Add(kvp.Key, kvp.Value);
            }
            foreach (KeyValuePair<ulong, long> kvp in sList3) {
                if (sList2to3.ContainsKey(kvp.Key) == false) {
                    sList2to3.Add(kvp.Key, kvp.Value);
                }
            }

            Logger.Log("Sets 1int2 contains " + sList1to2.Count.ToString("N0") + ", 1int3 contains " + sList1to3.Count.ToString("N0") + " and 2Un3 contains " + sList2to3.Count.ToString("N0") + " objects.");
            tsSortedListUse2 = new TimeSpan(DateTime.Now.Ticks).Subtract(new TimeSpan(start.Ticks));

            // clear all the data from memory ...
            sList1 = sList2 = sList3 = sList1to2 = sList1to3 = sList2to3 = null;
            Thread.Sleep(1000);


        }

        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>
        ///     Test the reading, writing and using of HashSet data structures
        /// </summary>
        public static void TestHashSets() {
            Logger.LogSubHeading("Testing HashSets ...");

            Thread.Sleep(1000);

            //-----WRITE-----
            Logger.Log("Writing HashSet data...");
            DateTime start = DateTime.Now;

            long totalMB4 = GC.GetTotalMemory(true);
            set1 = new HashSet<ulong>();
            foreach (TestObj rto in data1) {
                set1.Add(rto.ID);
            }
            long totalMAfter = GC.GetTotalMemory(true);
            long diff = totalMAfter - totalMB4;

            set2 = new HashSet<ulong>();
            foreach (TestObj rto in data2) {
                set2.Add(rto.ID);
            }
            set3 = new HashSet<ulong>();
            foreach (TestObj rto in data3) {
                set3.Add(rto.ID);
            }

            Logger.Log("Wrote three structures containing " + set1.Count.ToString("N0") + ", " + set2.Count.ToString("N0") + " and " + set3.Count.ToString("N0") + " objects.");
            Logger.Log("One structure consumed approximately " + diff.ToString("N0") + " bytes of memory.");
            tsHashSetWrite = new TimeSpan(DateTime.Now.Ticks).Subtract(new TimeSpan(start.Ticks));


            //-----READ-----
            Logger.Log("Reading HashSet data...");
            start = DateTime.Now;

            long counter = 0;
            foreach (long key in set1) {
                counter += key;
            }

            Logger.Log("Read and counted all the IDs, which total " + counter.ToString("N0") + ".");
            tsHashSetRead = new TimeSpan(DateTime.Now.Ticks).Subtract(new TimeSpan(start.Ticks));


            //-----USE-CONTAINS-----
            Logger.Log("Using HashSet data (Contains) ...");
            start = DateTime.Now;

            HashSet<ulong> hashSetUse = new HashSet<ulong>();
            foreach (ulong lo in set1) {
                if (hashSetUse.Contains(lo) == false) {
                    hashSetUse.Add(lo);
                }
            }

            Logger.Log("First object contained " + hashSetUse.Count.ToString("N0") + " unique values ...");
            tsHashSetUse1 = new TimeSpan(DateTime.Now.Ticks).Subtract(new TimeSpan(start.Ticks));


            //-----USE-MEMBERSHIP-----
            Logger.Log("Using HashSet data (Membership) ...");
            start = DateTime.Now;

            // Do two big intersections
            set1.IntersectWith(set2);
            set1.IntersectWith(set3);
            // and one union ...
            set2.UnionWith(set3);

            Logger.Log("Sets 1int2 / 1int3 contains " + set1.Count.ToString("N0") + " and 2Un3 contains " + set2.Count.ToString("N0") 
                + " objects.  Note that sets intersect / union is conducted with a native method - all the other structures require an extension. The calculation is a bit different from the other structures, so the results of the first number will be different.");
            tsHashSetUse2 = new TimeSpan(DateTime.Now.Ticks).Subtract(new TimeSpan(start.Ticks));

            // clear all the data from memory ...
            set1 = set2 = set3 = null;
            Thread.Sleep(1000);


        }



        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>
        ///     Test the reading, writing and using of Dictionary data structures
        /// </summary>
        public static void TestDictionaries() {
            Logger.LogSubHeading("Testing Dictionaries ...");

            Thread.Sleep(1000);

            //-----WRITE-----
            Logger.Log("Writing Dictionary data...");
            DateTime start = DateTime.Now;

            long totalMB4 = GC.GetTotalMemory(true);
            dict1 = new Dictionary<ulong, long>();
            foreach (TestObj rto in data1) {
                dict1.Add(rto.ID, rto.TestLong);
            }
            long totalMAfter = GC.GetTotalMemory(true);
            long diff = totalMAfter - totalMB4;

            dict2 = new Dictionary<ulong, long>();
            foreach (TestObj rto in data2) {
                dict2.Add(rto.ID, rto.TestLong);
            }
            dict3 = new Dictionary<ulong, long>();
            foreach (TestObj rto in data3) {
                dict3.Add(rto.ID, rto.TestLong);
            }

            Logger.Log("Wrote three structures containing "+dict1.Count.ToString("N0") + ", "+dict2.Count.ToString("N0") + " and "+dict3.Count.ToString("N0") + " objects.");
            Logger.Log("One structure consumed approximately " + diff.ToString("N0") + " bytes of memory.");
            tsDictionaryWrite = new TimeSpan(DateTime.Now.Ticks).Subtract(new TimeSpan(start.Ticks));


            //-----READ-----
            Logger.Log("Reading Dictionary data...");
            start = DateTime.Now;

            ulong counter = 0;
            foreach (KeyValuePair<ulong, long> kvp in dict1) {
                counter += kvp.Key;
            }

            Logger.Log("Read and counted all the IDs, which total "+counter.ToString("N0")+".");
            tsDictionaryRead = new TimeSpan(DateTime.Now.Ticks).Subtract(new TimeSpan(start.Ticks));


            //-----USE-CONTAINS-----
            Logger.Log("Using Dictionary data (Contains) ...");
            start = DateTime.Now;

            Dictionary<ulong, long> dictUse = new Dictionary<ulong, long>();
            foreach (KeyValuePair<ulong, long> kvp in dict1) {
                if (dictUse.ContainsKey(kvp.Key) == false) {
                    dictUse.Add(kvp.Key, kvp.Value);
                }
            }

            Logger.Log("First object contained "+dictUse.Count.ToString("N0")+" unique values ...");
            tsDictionaryUse1 = new TimeSpan(DateTime.Now.Ticks).Subtract(new TimeSpan(start.Ticks));


            //-----USE-MEMBERSHIP-----
            Logger.Log("Using Dictionary data (Membership) ...");
            start = DateTime.Now;

            // Do two big intersections - Unfortunately SortedList intersections are based on the key AND the value - this is the default equity comparer!!!!
            // so we cannot use the built in intersect and union methods out of the box
            Dictionary<ulong, long> dict1to2 = new Dictionary<ulong, long>();
            foreach (KeyValuePair<ulong, long> kvp in dict1) {
                if (dict2.ContainsKey(kvp.Key) == true) {
                    if (dict1to2.ContainsKey(kvp.Key) == false) {
                        dict1to2.Add(kvp.Key, kvp.Value);
                    } else {
                        dict1to2[kvp.Key] = kvp.Value;
                    }
                }
            }

            Dictionary<ulong, long> dict1to3 = new Dictionary<ulong, long>();
            foreach (KeyValuePair<ulong, long> kvp in dict1) {
                if (dict3.ContainsKey(kvp.Key) == true) {
                    if (dict1to3.ContainsKey(kvp.Key) == false) {
                        dict1to3.Add(kvp.Key, kvp.Value);
                    } else {
                        dict1to3[kvp.Key] = kvp.Value;
                    }
                }
            }


            // and one union ... conduct by adding all of the first list, and then any from the second list that dont match
            Dictionary<ulong, long> dict2to3 = new Dictionary<ulong, long>();
            foreach (KeyValuePair<ulong, long> kvp in dict2) {
                dict2to3.Add(kvp.Key, kvp.Value);
            }
            foreach (KeyValuePair<ulong, long> kvp in dict3) {
                if (dict2to3.ContainsKey(kvp.Key) == false) {
                    dict2to3.Add(kvp.Key, kvp.Value);
                }
            }

            Logger.Log("Sets 1int2 contains " + dict1to2.Count.ToString("N0") + ", 1int3 contains " + dict1to3.Count.ToString("N0") + " and 2Un3 contains " + dict2to3.Count.ToString("N0") + " objects.");
            tsDictionaryUse2 = new TimeSpan(DateTime.Now.Ticks).Subtract(new TimeSpan(start.Ticks));

            // clear all the data from memory ...
            dict1 = dict2 = dict3 = dict1to2 = dict1to3 = dict2to3 = null;
            Thread.Sleep(1000);
            

        }


        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>
        ///     Test the reading, writing and using of HashSet data structures
        /// </summary>
        public static void TestHashSetListCombo() {
            Logger.LogSubHeading("Testing HashSets with Lists ...");

            Thread.Sleep(1000);

            //-----WRITE-----
            Logger.Log("Writing HashSet data...");
            DateTime start = DateTime.Now;
            set1 = new HashSet<ulong>();
            foreach (TestObj rto in data1) {
                set1.Add(rto.ID);
            }
            list1 = new List<ulong>();
            foreach (TestObj rto in data1) {
                list1.Add(rto.ID);
            }

            set2 = new HashSet<ulong>();
            foreach (TestObj rto in data2) {
                set2.Add(rto.ID);
            }
            list2 = new List<ulong>();
            foreach (TestObj rto in data1) {
                list2.Add(rto.ID);
            }

            set3 = new HashSet<ulong>();
            foreach (TestObj rto in data3) {
                set3.Add(rto.ID);
            }
            list3 = new List<ulong>();
            foreach (TestObj rto in data1) {
                list3.Add(rto.ID);
            }


            Logger.Log("Wrote three structures containing " + set1.Count.ToString("N0") + ", " + set2.Count.ToString("N0") + " and " + set3.Count.ToString("N0") + " objects.");
            tsHashSetListWrite = new TimeSpan(DateTime.Now.Ticks).Subtract(new TimeSpan(start.Ticks));


            //-----USE-MEMBERSHIP-----
            Logger.Log("Using HashSet data (Membership) and comparing with Lists ...");
            start = DateTime.Now;

            // Do two big intersections
            set1.IntersectWith(list2);
            set2.IntersectWith(list3);
            // and one union ...
            set3.UnionWith(list2);

            Logger.Log("Sets 1int2 contains " + set1.Count.ToString("N0") + ", set 2int3 contains "+ set2.Count.ToString("N0") + "  and 2Un3 contains " + set3.Count.ToString("N0")
                + " objects.  Note that sets intersect / union is conducted with a native method - all the other structures require an extension. The calculation is a bit different from the other structures, so the results of the first number will be different.");
            tsHashSetListUse = new TimeSpan(DateTime.Now.Ticks).Subtract(new TimeSpan(start.Ticks));

            // clear all the data from memory ...
            set1 = set2 = set3 = null;
            Thread.Sleep(1000);


        }


        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>
        ///     Tests the combining of lists as intersection or union operations ...
        /// </summary>
        public static void TestListCombining() {

            Logger.LogSubHeading("Testing the List combining algorithm ...");

            // ok three lists of 1000, 10000 and 100000 done multiple times
            // Also try to increase this by a factor of 10 too ..

            /* 
             *      Note that the lists wont exactly be this length because of possible duplicates:
             *      Q: How many iterations does it take to cover a range with random values?
             *      A: This is known as the coupon collector's problem. See the link for details and references. 
             *      Asymptotically, the expected number of trials needed grows as n ln n (plus lower order terms).
             *      ln is the natural logarithm - see https://en.wikipedia.org/wiki/Natural_logarithm
             *      
             *      More on the coupon collectors problem - https://en.wikipedia.org/wiki/Coupon_collector%27s_problem
             *      For example, when n = 50 it takes about 225 trials to collect all 50 coupons.
             *      This is calculated as 1/probability1 + 1/probability2 + 1/probabilityN     e.g. 1/ (Num already chosen/Total Number)
             *      The upshot is that 1000 iterations of a range of randomly generated numbers between 1 and 1000, is likely to produce around 631 unique values.  For 10,000, its around 6,320...
             *      So how close we get to this (not too high OR low), is a good indication that our random number generator is on the money....
            */
            int numIterations = 100;
            int list1Length = 1000000; // 100000;
            int list2Length = 10000; // 10000;
            int list3Length = 100000; // 1000000;

            int rangeMin = 1;
            int rangeMax = 1000000;

            RedisWrapper rw = new RedisWrapper();

            Logger.Log("Running " + numIterations + " iterations with three lists of " + list1Length.ToString("N0") + ", "
                + list2Length.ToString("N0") + " and " + list3Length.ToString("N0") + " randomly generated integers ...");

            DateTime start = DateTime.Now;


            for (int i = 0; i < numIterations; i++) {

                //-----a----- Generate the random data ...
                Logger.Log("Iteration " + i + " - Generating the random data ...");

                List<List<uint>> listOfLists = new List<List<uint>>();
                List<uint> outputIntersection = new List<uint>();
                List<uint> outputUnion = new List<uint>();


                // List 1 ... we use hashsets to ensure no duplicates...
                HashSet<uint> list1 = new HashSet<uint>();
                for (int j = 0; j < list1Length; j++) {
                    list1.Add((uint)MGLEncryption.GenerateRandomInt(rangeMin, rangeMax));
                }
                listOfLists.Add(list1.ToList());

                // List 2 ...
                HashSet<uint> list2 = new HashSet<uint>();
                for (int j = 0; j < list2Length; j++) {
                    list2.Add((uint)MGLEncryption.GenerateRandomInt(rangeMin, rangeMax));
                }
                listOfLists.Add(list2.ToList());

                // List 3 ...
                HashSet<uint> list3 = new HashSet<uint>();
                for (int j = 0; j < list3Length; j++) {
                    list3.Add((uint)MGLEncryption.GenerateRandomInt(rangeMin, rangeMax));
                }
                listOfLists.Add(list3.ToList());

                Logger.Log("Iteration " + i + " has the following number of unique values for each list: " + list1.Count.ToString("N0") + ", "
                    + list2.Count.ToString("N0") + " and " + list3.Count.ToString("N0") + ".");


                // Now combine the lists .... Intersection
                outputIntersection = DataStructures.CombineLists(listOfLists, true);
                // And union
                outputUnion = DataStructures.CombineLists(listOfLists, false);

                Logger.Log("Iteration " + i + " had " + outputIntersection.Count + " matching by intersection and " + outputUnion.Count + " by union.");

            }

            TimeSpan procTime = DateTime.Now.Subtract(start);
            Logger.Log("Completed testing the combining of lists in " + procTime.TotalMilliseconds.ToString("N0") + " seconds.");

        }




    }
}
