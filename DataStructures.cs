using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataNirvana.Database {
    //------------------------------------------------------------------------------------------------------------------------------------------------------------------
    /// <summary>
    ///     Name:      DataStructures
    ///     Author:     Edgar Scrase
    ///     Date:       August 2016
    ///     Version:    0.1
    ///     Description:
    ///                     Contains methods to build and combine C# Datastructures including Lists, HashSets, SortedLists and Dictionaries
    ///                     in the most efficient manner
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
    public static class DataStructures {

        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>
        ///     Combines a list of lists, either by intersection or union, using HashSets to compute the unique values
        /// </summary>
        /// <param name="listOfLists">A list of lists of uints</param>
        /// <param name="doIntersect">True if an intersection should be performed between the lists; false if a union should be performed instead.</param>
        /// <returns>The combined list</returns>
        public static List<uint> CombineLists(List<List<uint>> listOfLists, bool doIntersect) {
            List<uint> output = new List<uint>();

            //-----a------ Check if null or empty and quit if so...
            if (listOfLists != null && listOfLists.Count > 0) {

                //-----b------ Identify and prioritise the simple case of a single list ... Just one list so no fancy intersection or union required.
                if (listOfLists.Count == 1) {                    
                    output = listOfLists[0];
                } else {

                    //-----c------ Generate metadata about the lists we are combining 
                    // Starting an intersection with the smallest list or a union with the largest list brings around a 10% performance improvement, so this is worthwhile.

                    // This is our listMetaInfo - the first int is the index of the individual list and the second is its size
                    List<KeyValuePair<int, int>> listMetaInfo = new List<KeyValuePair<int, int>>();
                    int counter = 0;
                    int totalCount = 0;

                    foreach (List<uint> list in listOfLists) {
                        int currentCount = (list == null || list.Count == 0) ? 0 : list.Count;

                        // lets just return immediately if any of the lists are null or empty and this is an intersection
                        if (currentCount == 0 && doIntersect == true) {
                            return output;
                        }

                        totalCount += currentCount;
                        listMetaInfo.Add(new KeyValuePair<int, int>(counter++, currentCount));
                    }
                    listMetaInfo.Sort(CompareKeyValuePairByValue);
                    

                    /*
                     *  -----d------
                     *  There is no benefit in using List.Intersect instead of HashSet.IntersectWith - the hashSets are always faster as the intersect operation is so quick with 
                     *  HashSets that it cancels out the performance penalty of slow writes.  The memory handling is also better as we dont need to create another list -
                     *  in HashSets all the computation is performed within the object.  See OneNote for some example timing comparisons ...
                    */

                    // Here is the HashSet that will do all the heaving lifting
                    HashSet<uint> hs = new HashSet<uint>();

                    if (doIntersect == true) {                                                      //-----d1----- ***** INTERSECTION *****

                        // Union the smallest list into the HashSet to get things started and then IntersectWith the other lists
                        counter = 0;
                        foreach (KeyValuePair<int, int> kvp in listMetaInfo) {
                            if (counter == 0) {
                                hs.UnionWith(listOfLists[kvp.Key]);
                            } else {
                                // Intersects is a O(M+N) operation so not tremendously fast ... http://stackoverflow.com/questions/14527595/intersection-of-two-sets-in-most-optimized-way
                                // But IntersectsWith works faster as the HashSet has builtin indexing.
                                hs.IntersectWith(listOfLists[kvp.Key]);

                                // Lets just return immediately if the current state of the intersection is now empty
                                if (hs.Count == 0) {
                                    return output;
                                }
                            }
                            counter++;
                        }

                    } else {                                                                        //-----d2----- ***** UNION *****

                        // Go with the largest list first to optimise the union method
                        for (counter = (listMetaInfo.Count - 1); counter >= 0; counter--) {
                            hs.UnionWith(listOfLists[listMetaInfo[counter].Key]);
                        }

                    }
                    // lets get out of here!!
                    return hs.ToList();
                }
            }

            // The default return
            return output;
        }


        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>
        ///     A comparer method to compare two key value pairs by their keys ...
        /// </summary>
        public static int CompareKeyValuePairByKey(KeyValuePair<int, int> a, KeyValuePair<int, int> b) {
            return a.Key.CompareTo(b.Key);
        }

        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>
        ///     A comparer method to compare two key value pairs by their values ...
        /// </summary>
        public static int CompareKeyValuePairByValue(KeyValuePair<int, int> a, KeyValuePair<int, int> b) {
            return a.Value.CompareTo(b.Value);
        }


        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        private static int BinarySearch<T>(IList<T> list, T value) {
            int lo = -1;//////////////////////////////////////////////////

            if (list != null && list.Count > 0) {
                var comp = Comparer<T>.Default;

                // check that our value is actually within the scope of this list!!
                if (comp.Compare(value, list[0]) >= 0 && comp.Compare(value, list[list.Count - 1]) <= 0) {
                    lo = 0;
                    int hi = list.Count - 1;
                    while (lo < hi) {
                        int m = (hi + lo) / 2;  // this might overflow; be careful.
                        if (comp.Compare(list[m], value) < 0) {
                            lo = m + 1;
                        } else {
                            hi = m - 1;
                        }
                    }
                    if (comp.Compare(list[lo], value) < 0) {
                        lo++;
                    }
                }
            }
            return lo;
        }

        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        public static int FindFirstIndexGreaterThanOrEqualTo<T, U>(this SortedList<T, U> sortedList, T key) {
            return BinarySearch(sortedList.Keys, key);
        }


    }
}
