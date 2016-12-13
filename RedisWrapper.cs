using DataNirvana.DomainModel.Database;
using MGL.Data.DataUtilities;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using System.Xml;

//---------------------------------------------------------------------------------------------------------------------------------------------------------------------
namespace DataNirvana.Database {
    //------------------------------------------------------------------------------------------------------------------------------------------------------------------
    /// <summary>
    ///     Name:      RedisWrapper
    ///     Author:     Edgar Scrase
    ///     Date:       July and August 2016
    ///     Version:    0.5
    ///     Description:
    ///                     A wrapper around the StackExchange.Redis library in order to make the Redis database more similar to an RDBMS
    ///                     introducing the concepts of tables and indexes.  
    ///                     This will help to compare the performance of the two.
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
    public class RedisWrapper {

        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>
        ///     The connection multiplexer that handles all the database connections to Redis
        /// </summary>
        protected ConnectionMultiplexer CM = null;
        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>
        ///     Our database connections arrays - we pick these randomly when running queries as having more than one optimises the operations to and from Redis ...
        /// </summary>        
        protected List<IDatabase> rDBs = new List<IDatabase>();
        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>
        ///     The number of connections to the Redis database that we generated.  Having more than one stops the query buffer from being overloaded,
        ///     and seems to marginally improve performance.  Anymore than 10 seems to have no great value, and more than 100, seems to degrade the overall performance.
        /// </summary>
        public static int MaxNumConnections = 10;
        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>
        ///     A random approach to accessing specific database connections.  We tried using a threadsafe int incrementation approach, but
        ///     this current implementation produces better results by simply taking the current 0.nth part of a second.
        ///     This will get a number between 0 and 9 (inclusive) which should be fairly random as long as the connection requests are not timed for specific e.g. every second.
        /// </summary>       
        public int CurrentConnection {
            get {
                int c = (int)Math.Floor((double)DateTime.Now.Millisecond / 1000.0 * (double) MaxNumConnections);
                return c;
            }
        }
        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>
        ///     The list of hosts with their respective ports (e.g. localhost:6379) is stored once the Connect method has been called.
        /// </summary>
        protected List<string> rHostWithPorts = null;
        /// <summary>
        ///     Whether or not this is a cluster that is connected
        /// </summary>
        protected bool rCluster = false;
        /// <summary>
        ///     The ID of the database we are connected to.
        /// </summary>
        protected int rDBID = 0;
        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>
        ///     The default timeout.  See also the cluster node timeout parameter in the Redis config files.
        ///     For realtime activities, this could be set to be very low, but for more general purpose activities, we should include a bit of 
        ///     flexibility.  It is in milliseconds and about 336 seconds seems to cover even the heaviest of queries.
        /// </summary>
        public static int DefaultSyncTimeout = 336000;

        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>
        ///     If set to false, causes all async methods to be conducted synchronously.  This might result in better performance in a threaded approach.
        ///     However, Redis seems to almost always peform better using the async methods, due to the built in pipelining and multiplexing.
        /// </summary>
        protected static bool DoAsync = true; // false;

        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>
        ///     If set to true, stores additional RedisDebug information for each of the searches conducted, which can be useful when trying to optimise queries.
        /// </summary>
        public static bool DoDebug = false; // true;
        /// <summary>
        ///     The list of debug information that is generated for each search.
        /// </summary>
        public static List<DatabaseDebug> Debug = new List<DatabaseDebug>();
        /// <summary>
        ///     The list of index information for a given type of object.  This is only really used currently to support the debug options above.
        ///     Set this list with RedisWrapper.IndexFindAll.
        /// </summary>
        public static List<DatabaseIndexInfo> IndexInfo = new List<DatabaseIndexInfo>();

        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>
        ///     For search queries with multiple components, there are two general options, either we conduct both search patterns and then intersect
        ///     or union the results, OR we identify the smallest component and then scan each of the matching objects, to see if they match the 
        ///     subsequent patterns.  Set this to true to perform the latter approach
        /// </summary>
        protected static bool DoRangeScan = true;
        /// <summary>
        ///     For RangeScans, there is an upper and lower limit beyond which the performance degrades.  Somewhere between 100 and 1000 seems
        ///     good for general purpose queries.
        /// </summary>
        protected static int RangeScanThreshold = 1000; // 100;

        
        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>
        ///     When writing data, especially when also saving it on a disk, as the number of records increases, the async methods
        ///     may well fire too many queries causing the background saves to miss their schedule and Redis will generate a cannot save error.
        ///     
        ///     This parameter breaks up large chunks of data to be written (as the object Hashes or exposed indexes).  This helps minimise
        ///     memory usage and improve overall system stability.
        ///     
        ///     If not saving to disk, or working with fast SSD disks, this can be very high (e.g 5m).  Slower IDE disks might need a lower threshold.
        /// </summary>
        public static int WriteChunkSize = 5000000;
        /// <summary>
        ///     When writing data, especially when also saving it on a disk, as the number of records increases, the async methods
        ///     may well fire too many queries causing the background saves to miss their schedule and Redis will generate a cannot save error.
        ///     
        ///     This parameter causes the WriteDataAsHash method to pause for the specified number of milliseconds every n records, where n is specified by
        ///     the WriteHashPauseThreshold parameter.  
        ///     
        ///     If not saving to disk this can be close to zero.  For fast SSD disks, about 20 seconds works well.  Slower IDE disks are untested.
        /// </summary>
        public static int WriteHashPauseLength = 10;
        /// <summary>
        ///     When writing data, especially when also saving it on a disk, as the number of records increases, the async methods
        ///     may well fire too many queries causing the background saves to miss their schedule and Redis will generate a cannot save error.
        ///     
        ///     This parameter causes the WriteDataAsHash method to pause for the number of milliseconds specified in WriteHashPauseLength
        ///     every n records, where n is specified by this parameter.  
        ///     
        ///     If not saving to disk this can be very high (e.g 10m).  A rock solid threshold for very intensive usage would be around 1m.
        /// </summary>
        public static int WriteHashPauseThreshold = 10000000;
        /// <summary>
        ///     When writing data, especially when also saving it on a disk, as the number of records increases, the async methods
        ///     may well fire too many queries causing the background saves to miss their schedule and Redis will generate a cannot save error.
        ///     
        ///     This parameter causes the Index methods to pause for the specified number of milliseconds every n records, where n is specified by
        ///     the WriteIndexPauseThreshold parameter.  
        ///     
        ///     If not saving to disk this can be close to zero.  For fast SSD disks, about 1 seconds works well.  Slower IDE disks are untested.
        /// </summary>
        public static int WriteIndexPause = 10;
        /// <summary>
        ///     When writing data, especially when also saving it on a disk, as the number of records increases, the async methods
        ///     may well fire too many queries causing the background saves to miss their schedule and Redis will generate a cannot save error.
        ///     
        ///     This parameter causes the Index methods to pause for the number of milliseconds specified in WriteIndexPauseLength
        ///     every n records, where n is specified by this parameter.  
        ///     
        ///     If not saving to disk this can be very high (e.g 10m).  A rock solid threshold for very intensive usage would be around 1m.
        /// </summary>
        public static int WriteIndexPauseThreshold = 10000000;


        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>
        ///     The list of currently accepted property types.  Super simple for now. 
        /// </summary>
        protected List<string> acceptedPropertyTypes = new List<string>() { "Int32", "Int64", "UInt32", "UInt64", "Double", "Boolean", "String", "DateTime" };

        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>
        ///     Text indexes are stored as sets of IDs.  Each set is grouped based on the first n characters in the text string, where that length
        ///     n is specified by this parameter.
        /// </summary>
        public static int TextIndexLength = 2;

        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>
        ///     A constant used to generate the key names in a syntactically smashing way.  In this case this is the Primary key index suffix to append to the object name.
        ///     The primary key suffix should include upper case and be a little longer so that there is no possibiblity of it overlapping with the text set buckets ... 
        /// </summary>
        protected static readonly string IndexPrimaryKeySuffix = ":PKey";
        /// <summary>
        ///     A constant used to generate the key names in a syntactically smashing way.  In this case this is the generic numeric index suffix to append to the object name.
        /// </summary>
        protected static readonly string IndexNumeric = ":i:";
        /// <summary>
        ///     A constant used to generate the key names in a syntactically smashing way.  In this case this is the generic textual index suffix to append to the object name.
        ///     There is now no added value with these being different from the numeric indexes
        /// </summary>
        protected static readonly string IndexText = ":i:"; 
        /// <summary>
        ///     The lead hashtag - hashtags are useful in Redis as they force similar hashtags to be present on the same cluster node (shard?) which enables things like set 
        ///     concatenation to be performed.
        /// </summary>
        protected static readonly string HashTagStart = "{";
        /// <summary>
        ///     The closing hashtag - hashtags are useful in Redis as they force similar hashtags to be present on the same cluster node (shard?) which enables things like set 
        ///     concatenation to be performed.
        /// </summary>
        protected static readonly string HashTagEnd = "}";


        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>
        ///     Connects to a Redis cluster instance.
        /// </summary>
        /// <param name="hostsWithPorts">A list of the cluster nodes (hostname:port) e.g. localhost:6379</param>
        /// <param name="auth">The auth string (Redis uses a single auth string which is roughly equivalent to the database password in other contexts)</param>
        /// <param name="isCluster">Whether or not this is a cluster instance to connect to</param>
        /// /// <param name="allowAdmin">Whether or not to allow administrative queries</param>
        /// <param name="dbID">The database ID (in a cluster context this will always be 0)</param>
        /// <returns>True if the connection was successful</returns>
        public bool Connect(List<string> hostsWithPorts, string auth, bool allowAdmin, bool isCluster, int dbID) {
            bool success = false;

            //-----a----- Set the global parameters
            rCluster = isCluster;
            rDBID = dbID;

            //-----b----- Build the connection string - e.g. "localhost,password=XXX_FUCKING_GOOD_ONE_XXX,allowAdmin=true"
            StringBuilder connectionStr = new StringBuilder();
            /*
             * -----b1------
             *  The location of the Redis database to connect to (e.g. localhost or another IP address)
             *  Include the port as well ... - see http://stackoverflow.com/questions/29247616/how-does-stackexchange-redis-use-multiple-endpoints-and-connections
             *  e.g. localhost:6379 ... or localhost:6379,localhost:6380,localhost:6381,localhost:6382,localhost:6383,localhost:6384,localhost:6385,localhost:6386
            */
            if (hostsWithPorts != null) {
                foreach (string hwp in hostsWithPorts) {
                    if (connectionStr.Length > 0) {
                        connectionStr.Append(",");
                    }
                    connectionStr.Append(hwp);
                }
                rHostWithPorts = hostsWithPorts;
            } else {
                // Go with some very generic settings and warn
                Logger.LogWarning("No connection information provided, so going with the defaults - localhost and port 6379");
                connectionStr.Append("localhost:6379");
                rHostWithPorts = new List<string>();
                rHostWithPorts.Add(connectionStr.ToString());
            }

            //-----b2----- The connection timeout
            connectionStr.Append(",syncTimeout=" + DefaultSyncTimeout);

            //-----b3----- The auth password, if needed ...
            if (string.IsNullOrEmpty(auth) == false) {
                connectionStr.Append(",password=" + auth);
            }
            //-----b4----- Whether or not to allow admin activities
            connectionStr.Append(",allowAdmin=" + allowAdmin.ToString().ToLower());

            //-----c----- Lets disconnect if there already appears to be a connection
            if (CM == null || rDBs == null) {
                rDBs = new List<IDatabase>();
            } else {
                Disconnect();
            }

            //-----d----- Try to get the connection multiplexer and create a collection of connections
            CM = ConnectionMultiplexer.Connect(connectionStr.ToString());
            if (CM == null || CM.IsConnected == false) {
                Logger.LogError(5, "Could not connect to the Redis database using the connection multiplexer.  Check all the required Redis services have been started successfully.");
            } else {
                for (int i = 0; i < MaxNumConnections; i++) {
                    rDBs.Add(CM.GetDatabase(rDBID));
                }
            }

            //-----e----- This success metric is too simple at the moment - we should also test that the connections really work too.
            success = CM.IsConnected && rDBs.Count == MaxNumConnections;

            return success;
        }

        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>
        ///     Connects to a non-cluster Redis database instance.  
        /// </summary>
        /// <param name="hostName">The hostName on which the Redis database instance resides</param>
        /// <param name="port">The port ID through which to connect to the Redis database instance</param>
        /// <param name="auth">The auth string (Redis uses a single auth string which is roughly equivalent to the database password in other contexts)</param>
        /// <param name="dbID">The database ID (in a cluster context this will always be 0)</param>
        /// <param name="allowAdmin">Whether or not to allow administrative queries</param>
        /// <returns></returns>
        public bool Connect(string hostName, int port, string auth, int dbID, bool allowAdmin) {
            string hwp = hostName + ":" + port;
            return Connect(new List<string>() { hwp }, auth, allowAdmin, false, dbID);
        }
        

        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>
        ///     Randomly gets a database connection from our array ...  Testing the stability here !
        /// </summary>
        public IDatabase DB {
            get {
                if (rDBs == null || rDBs.Count == 0) {
                    return null;
                } else {
                    return rDBs[CurrentConnection];
                }
            }
        }


        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>
        ///     Disconnect from the Redis database instance by closing the connection multiplexer and setting the list of database connections to null.
        /// </summary>
        public void Disconnect() {

            // Note that this should automatically kill rServer and rDB, but we should check for this ....
            if ( CM != null) {
                CM.Close();
                CM.Dispose();
                rDBs = null;
            }
        }


        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>
        ///     Removes all data hashes, as well as supporting indexes etc
        /// </summary>
        /// <param name="objectName">The name of the data associated with this object to remove</param>
        public bool ClearData(string objectName) {
            bool success = true; // innocent until proven guilty

            Logger.Log("RedisWrapper.ClearData - Removing all objects relating to " + objectName + "...");
           
            //-----a------ Iterate through the keys on each server that match a pattern of objectName* or {objectName* ({ is the start of a hashtag)
            // e.g. localhost:6379 ...
            if (rHostWithPorts != null) {
                foreach (string hwp in rHostWithPorts) {

                    Logger.Log("Checking server " + hwp +" for keys to remove...");
                    int counter = 0;
                    int indexCount = 0;

                    //-----b----- Get the server from the connection multiplexer
                    IServer ts = CM.GetServer(hwp);

                    if (ts.IsConnected == true) {
                        // 15-Aug-2016 - Do this in two phases now that the object names are shorter and potentially not unique

                        //-----c----- First all objects starting with our objectname
                        foreach (var key in ts.Keys(rDBID, objectName + "*", 1000, CommandFlags.None)) {
                            Logger.Log(++counter, 1000);
                            success = success & rDBs[CurrentConnection].KeyDelete(key);
                        }
                        //-----d----- Secondly, all objects starting with a hash tage and then our objectname
                        foreach (var key in ts.Keys(rDBID, RedisWrapper.HashTagStart + objectName + "*", 1000, CommandFlags.None)) {
                            success = success & rDBs[CurrentConnection].KeyDelete(key);
                            Logger.Log(++counter, 1000);
                            indexCount++;
                        }
                        Logger.Log(++counter, 1);
                        Logger.Log("");
                        Logger.Log("Removed "+counter+" keys in total, including "+indexCount+" indexes.");
                    }
                }
            }

            return success;
        }


        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>
        ///     This is called a lot as a way of standardising the IDs quickly to a particular type.  We tested the memory usage of uint vs ulong
        ///     and uint unsurprisingly were much better.  Given that most built in c# lists are only good for 4billion entries too, uint should be good
        ///     enough for most of our use cases
        /// </summary>
        public uint ConvertID(string idStr) {
            uint id = 0;
            uint.TryParse(idStr, out id);
            return id;
        }
        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>
        ///     Note that this process of casting a RedisValue to a uint is MUCH faster than the heavier TryParse methods as long as the RedisValue
        ///     is a positive integer or long in the integer range.  Otherwise the exceptions will cause this to be much much slower!!
        ///     
        ///     This is called a lot as a way of standardising the IDs quickly to a particular type.  We tested the memory usage of uint vs ulong
        ///     and uint unsurprisingly were much better.  Given that most built in c# lists are only good for 4billion entries too, uint should be good
        ///     enough for most of our use cases.
        /// </summary>
        public uint ConvertID(RedisValue idNum) {
            return (uint)idNum;
        }


        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>
        ///     Write a list of objects to a database
        ///     just simple object wrappers containing int, double, long, string and dateTime????
        ///     would need to have the relevant get and set methods
        ///     what about lists of the above in the array???
        ///     would have to handle objects as parameters recursively and go into each until we get down to the base information ...
        ///     Note that this is not yet asyn!!!
        /// </summary>
        /// <param name="objList"></param>
        /// <returns></returns>
        public bool WriteDataAsHash(List<object> objList) {
            bool success = true; // innocent until proven guilty

            // No point continuing if the given object list is null ...
            if (objList == null) {
                success = false;
            } else {

                //-----a----- Use reflection to get the name of the object ...
                string objName = null;
                PropertyInfo[] objPI = null;
                GetObjectProperties(objList[0], out objName, out objPI);

                //-----b----- And lets get started - we need to declare our lists of tasks and setup the chunking parameters
                Logger.Log("Starting to write data for " + objList.Count + " objects of type " + objName);
                List<Task<bool>> allSetDataAdded = new List<Task<bool>>();
                List<Task> allHashDataAdded = new List<Task>();

                int totalCount = objList.Count;
                int counter = 0;
                int chunkSize = RedisWrapper.WriteChunkSize;
                int startIndex = 0;

                //-----c----- And then iterate through the data in chunks 
                while (counter < totalCount) {

                    //-----c1----- Catch the cases where the number of results is less than our chunkSize
                    chunkSize = ((startIndex + chunkSize) <= totalCount) ? chunkSize : (totalCount - startIndex);
                    Logger.Log("Processing a " + chunkSize + " object chunk, starting at index " + startIndex + ".");

                    //-----c2----- iterate through the objects in this chunk ...
                    for (int i = startIndex; i < startIndex + chunkSize; i++) {
                        object obj = objList[i];
                        List<KeyValuePair<string, string>> objData = ParseObject(obj);

                        //-----c3----- Now write the data as a hash ....
                        List<HashEntry> heList = new List<HashEntry>();
                        RedisValue id = 0;
                        foreach (KeyValuePair<string, string> kvp in objData) {
                            heList.Add(new HashEntry(kvp.Key, kvp.Value));

                            // Special case here is the ID - we always want to make sure this is a uint so that all comparisons using RedisValues are consistent
                            // (RedisValue of long vs int will return not matching even if the values are the same) 
                            if (kvp.Key.Equals("ID", StringComparison.CurrentCultureIgnoreCase) == true) {
                                id = ConvertID(kvp.Value);
                            }
                        }
                        // As the ID is so important, we want to ensure it is set, so if the ID is 0, lets set it to the counter ...
                        if (id == 0) {
                            id = counter;
                        }

                        //-----c4----- Add the info async so it does not block this code, firstly the primary key to our set
                        allSetDataAdded.Add(rDBs[CurrentConnection].SetAddAsync(KeyName(objName), id));
                        //-----c5----- Then the data as a hash ..
                        allHashDataAdded.Add(rDBs[CurrentConnection].HashSetAsync(KeyName(objName, ConvertID(id)), heList.ToArray()));

                        Logger.Log(++counter, 1000, objList.Count);
                    }
                    Logger.Log("");

                    //-----d1----- And then lets await the completion of these two lists of tasks
                    success = success & AwaitTasks(allSetDataAdded, " adding the primary key to the index set for " + objName + ".");
                    success = success & AwaitTasks(allHashDataAdded, " adding the hashSets for " + objName + ".");

                    //-----e1----- Reset and garbage collect
                    allSetDataAdded = new List<Task<bool>>();
                    allHashDataAdded = new List<Task>();
                    GC.Collect();

                    //-----e2----- Pause if there is a decent wodge of data
                    if (totalCount > RedisWrapper.WriteHashPauseThreshold) {
                        Logger.Log("Pausing for a breather...");
                        Thread.Sleep(RedisWrapper.WriteHashPauseLength);
                    }

                    //-----e3----- Set for the next chunk....
                    startIndex += chunkSize;
                }
            }

            return success;
        }

        //-----------------------------------------------------------------------------------------------------------------------------------------------------------
        // also need an add object method, which does all the indexing as well...
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        // also need an remove object method, which also removes all the indexes too
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        //-----------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>
        ///         Uses reflection to serialise all of the exposed attributes of the given object.
        ///         Really only designed to work with primitive data types
        /// </summary>
        public List<KeyValuePair<string, string>> ParseObject(object obj) {
            List<KeyValuePair<string, string>> values = null;

            try {

                /* 
                    Note that this is effectively a lightweight serialisation of the objects here, we could instead use the built in serialisation if we need
                    to work with more complicated objects - see https://support.microsoft.com/en-za/kb/815813
                    
                    StreamReader s = new StreamReader(;
                    StringBuilder output = new StringBuilder();
                    XmlWriter xmlW = XmlWriter.Create(output);
                    System.Xml.Serialization.XmlSerializer x = new System.Xml.Serialization.XmlSerializer(obj.GetType());
                    x.Serialize(xmlW, obj);
                */

                //-----a----- Get the attributes of our object through reflection
                string objName = null;
                PropertyInfo[] objPI = null;
                GetObjectProperties(obj, out objName, out objPI);

                //-----b----- Loop through all the exposed properties of our object and store them in a list of KeyValuePairs by 
                // using the .ToString method of the generic object
                object sourceVal = null;
                values = new List<KeyValuePair<string, string>>();

                foreach (PropertyInfo propInfo in objPI) {
                    string propName = propInfo.Name;
                    sourceVal = propInfo.GetValue(obj, null);

                    // Warn if the property type is not supported
                    if (acceptedPropertyTypes.Contains(propInfo.PropertyType.Name) == false) {
                        Logger.Log("Property "+propInfo.Name+" is of an unsupported type: " + propInfo.PropertyType.Name);
                    }

                    if (sourceVal != null) {
                        // Special cases - convert the DateTime to ticks as we will store this instead and store the bools as 0 or 1 ...
                        if (propInfo.PropertyType.Name.Equals("DateTime", StringComparison.CurrentCultureIgnoreCase) == true) {
                            sourceVal = ((DateTime)sourceVal).Ticks;
                        } else if (propInfo.PropertyType.Name.Equals("Boolean", StringComparison.CurrentCultureIgnoreCase) == true) {
                            sourceVal = ((bool)sourceVal == true) ? 1 : 0;
                        }

                    } else {
                        sourceVal = "";
                    }

                    // And then add our value to the list of property name value key value pairs
                    values.Add(new KeyValuePair<string, string>(propName, sourceVal.ToString()));
                }

            } catch (Exception ex) {
                Logger.LogError(8, "ParseObject: Error cloning the object (" + LoggerErrorTypes.Parsing + "):" + ex.ToString());
            }

            return values;
        }


        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>
        ///     Indexes a specific field of an object in the database.
        ///     Data is extracted from the objects stored as Hashes in the database of the form "objName:ID"
        /// </summary>
        /// <param name="objectName">Name of the object to index</param>
        /// <param name="propertyName">Name of the property in the object to index</param>
        /// <param name="propertyType">The type of the property (e.g. Uint32, String, etc.)</param>
        /// <returns>True if the indexing has completed successfully</returns>
        public bool IndexNumericProperty(string objectName, string propertyName, string propertyType) {
            return IndexNumericProperty(objectName, propertyName, propertyType, BuildNumericList(objectName, propertyName, false));
        }
        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>
        ///     Indexes a specific field of an object in the database.
        ///     The index data is provided in the data parameter.
        /// </summary>
        /// <param name="objectName">Name of the object to index</param>
        /// <param name="propertyName">Name of the property in the object to index</param>
        /// <param name="propertyType">The type of the property (e.g. Uint32, String, etc.)</param>
        /// <param name="data">The data used to build the index.  The keys in the KeyValuePairs and the values contain the numeric information.</param>
        /// <returns>True if the indexing has completed successfully</returns>
        public bool IndexNumericProperty(string objectName, string propertyName, string propertyType, List<KeyValuePair<uint, double>> data) {
            bool success = true; // innocent until proven guilty

            List<Task<bool>> tasks = new List<Task<bool>>();

            //-----a----- Do a warning here about unsupported property types
            if (acceptedPropertyTypes.Contains(propertyType) == false) {
                Logger.LogWarning("IndexNumericProperty - unsupported type found for property " + propertyName + " in object " + objectName + "! " + propertyType);
            }

            //-----b1----- Generate the name of the sorted Set
            // e.g. MyObject:i:MyProperty or HashTagStart + objectName + IndexNumeric + propertyName + HashTagEnd
            string setName = KeyName(objectName, propertyName, SearchType.Score); 

            //-----b2----- And now lets write a list of the unique values for this index in a sortedSet in a series of chunks
            int totalCount = data.Count;
            int counter = 0;
            int chunkSize = RedisWrapper.WriteChunkSize;
            int startIndex = 0;

            //-----c----- Iterate through the chunks
            while (counter < totalCount) {

                //-----c1----- Catch the cases where the number of results is less than our chunkSize
                chunkSize = ((startIndex + chunkSize) <= totalCount) ? chunkSize : (totalCount - startIndex);
                Logger.Log("Processing a " + chunkSize + " object chunk, starting at index " + startIndex + ".");

                //-----c2----- Iterate through the objects in this chunk and add them to a sorted set asyncronously ...
                for (int i = startIndex; i < startIndex + chunkSize; i++) {
                    KeyValuePair<uint, double> kvp = data[i];

                    // Add this task to our list of tasks in this chunk
                    tasks.Add(rDBs[CurrentConnection].SortedSetAddAsync(setName, kvp.Key, kvp.Value, CommandFlags.None));

                    Logger.Log(++counter, 1000, data.Count);
                }
                Logger.Log("");

                //-----d----- Check it all ran successfully in a soft manner using the AwaitTasks approach
                success = success & AwaitTasks(tasks, "indexing property " + propertyName + " for object " + objectName + ".");

                //-----e1----- Reset and garbage collect
                tasks = new List<Task<bool>>();
                GC.Collect();

                //-----e2----- Pause if there is a decent wodge of data
                if (totalCount > RedisWrapper.WriteIndexPauseThreshold) {
                    Logger.Log("Pausing for a breather...");
                    Thread.Sleep(RedisWrapper.WriteIndexPause);
                }
                //-----e3----- Set for the next chunk....
                startIndex += chunkSize;
            }

            //-----f----- Before leaving the method, lets kill what we can and call garbage collection to minimise the memory usage if possible
            tasks = null;
            GC.Collect();

            return success;
        }


        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>
        ///     Indexes a specific Boolean field of an object in the database.
        ///     Data is extracted from the objects stored as Hashes in the database of the form "objName:ID"
        /// </summary>
        /// <param name="objectName">Name of the object to index</param>
        /// <param name="propertyName">Name of the property in the object to index</param>
        /// <returns>True if the indexing has completed successfully</returns>
        public bool IndexBool(string objectName, string propertyName) {
            return IndexBool(objectName, propertyName, BuildNumericList(objectName, propertyName, false));
        }
        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>
        ///     Indexes a specific Boolean field of an object in the database.
        ///     The index data is provided in the data parameter.
        /// </summary>
        /// <param name="objectName">Name of the object to index</param>
        /// <param name="propertyName">Name of the property in the object to index</param>
        /// <param name="data">The data used to build the index.  The keys in the KeyValuePairs and the values contain the numeric information.  
        /// In this case the bool information is stored as 0 or 1.</param>
        /// <returns>True if the indexing has completed successfully</returns>
        public bool IndexBool(string objectName, string propertyName, List<KeyValuePair<uint, double>> data) {
            bool success = false;

            //-----b1----- Generate the stub of the set name for our two sets (0 or 1)
            // e.g. MyObject:i:MyProperty:0 or HashTagStart + objectName + IndexNumeric + propertyName + HashTagEnd + ":"
            string setNameStub = KeyName(objectName, propertyName, SearchType.Bool);

            //-----b2----- Instantiate our chunk parameters
            List<Task<bool>> tasks = new List<Task<bool>>();
            int totalCount = data.Count;
            int counter = 0;
            int chunkSize = RedisWrapper.WriteChunkSize;
            int startIndex = 0;

            //-----c----- Iterate through the chunks
            while (counter < totalCount) {

                //-----c1----- Catch the cases where the number of results is less than our chunkSize
                chunkSize = ((startIndex + chunkSize) <= totalCount) ? chunkSize : (totalCount - startIndex);
                Logger.Log("Processing a " + chunkSize + " object chunk, starting at index " + startIndex + ".");

                //-----c2----- Iterate through the objects in this chunk and add them to a set asyncronously ...
                for (int i = startIndex; i < startIndex + chunkSize; i++) {
                    KeyValuePair<uint, double> kvp = data[i];

                    int keyNum = (int)kvp.Value;
                    tasks.Add(rDBs[CurrentConnection].SetAddAsync(setNameStub + keyNum, kvp.Key));

                    Logger.Log(++counter, 1000, data.Count);
                }
                Logger.Log("");

                //-----d----- Check it all ran successfully in a soft manner using the AwaitTasks approach
                success = success & AwaitTasks(tasks, "indexing property " + propertyName + " for object " + objectName + ".");

                //-----e1----- Reset and garbage collect
                tasks = new List<Task<bool>>();
                GC.Collect();

                //-----e2----- Pause if there is a decent wodge of data
                if (totalCount > RedisWrapper.WriteIndexPauseThreshold) {
                    Logger.Log("Pausing for a breather...");
                    Thread.Sleep(RedisWrapper.WriteIndexPause);
                }

                //-----e3----- Set for the next chunk....
                startIndex += chunkSize;
            }

            //-----f----- Before leaving the method, lets kill what we can and call garbage collection to minimise the memory usage if possible
            tasks = null;
            GC.Collect();

            return success;
        }


        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>
        ///     Indexes a specific DateTime field of an object in the database.
        ///     Data is extracted from the objects stored as Hashes in the database of the form "objName:ID"
        /// </summary>
        /// <param name="objectName">Name of the object to index</param>
        /// <param name="propertyName">Name of the property in the object to index</param>
        /// <returns>True if the indexing has completed successfully</returns>
        public bool IndexDateTime(string objectName, string propertyName) {
            return IndexDateTime(objectName, propertyName, BuildNumericList(objectName, propertyName, false));
        }
        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>
        ///     Indexes a specific DateTime field of an object in the database.
        ///     The index data is provided in the data parameter.
        /// </summary>
        /// <param name="objectName">Name of the object to index</param>
        /// <param name="propertyName">Name of the property in the object to index</param>
        /// <param name="data">The data used to build the index.  The keys in the KeyValuePairs and the values contain the numeric information.  
        /// In this case the DateTime information is stored as long ticks.</param>
        /// <returns>True if the indexing has completed successfully</returns>
        public bool IndexDateTime(string objectName, string propertyName, List<KeyValuePair<uint, double>> data) {
            bool success = false;

            //-----b1----- Generate the sorted set name
            // e.g. MyObject:i:MyProperty or HashTagStart + objectName + IndexNumeric + propertyName + HashTagEnd
            string sortedSetName = KeyName(objectName, propertyName, SearchType.DateTime);

            //-----b2----- Instantiate our chunk parameters
            List<Task<bool>> tasks = new List<Task<bool>>();
            int totalCount = data.Count;
            int counter = 0;
            int chunkSize = RedisWrapper.WriteChunkSize;
            int startIndex = 0;

            //-----c----- Iterate through the chunks
            while (counter < totalCount) {

                //-----c1----- Catch the cases where the number of results is less than our chunkSize
                chunkSize = ((startIndex + chunkSize) <= totalCount) ? chunkSize : (totalCount - startIndex);
                Logger.Log("Processing a " + chunkSize + " object chunk, starting at index " + startIndex + ".");

                //-----c2----- Iterate through the objects in this chunk and add them to the sorted set asyncronously ...
                for (int i = startIndex; i < startIndex + chunkSize; i++) {
                    KeyValuePair<uint, double> kvp = data[i];

                    long ticks = (long)kvp.Value;
                    tasks.Add(rDBs[CurrentConnection].SortedSetAddAsync(sortedSetName, kvp.Key, ticks));

                    Logger.Log(++counter, 1000, data.Count);
                }
                Logger.Log("");

                //-----d----- Check it all ran successfully in a soft manner using the AwaitTasks approach
                success = success & AwaitTasks(tasks, "indexing property " + propertyName + " for object " + objectName + ".");

                //-----e1----- Reset and garbage collect
                tasks = new List<Task<bool>>();
                GC.Collect();

                //-----e2----- Pause if there is a decent wodge of data
                if (totalCount > RedisWrapper.WriteIndexPauseThreshold) {
                    Logger.Log("Pausing for a breather...");
                    Thread.Sleep(RedisWrapper.WriteIndexPause);
                }

                //-----e3----- Set for the next chunk....
                startIndex += chunkSize;
            }


            //-----f----- Before leaving the method, lets kill what we can and call garbage collection to minimise the memory usage if possible
            tasks = null;
            GC.Collect();

            return success;
        }


        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>
        ///     Indexes a specific String field of an object in the database.
        ///     Data is extracted from the objects stored as Hashes in the database of the form "objName:ID"
        /// </summary>
        /// <param name="objectName">Name of the object to index</param>
        /// <param name="propertyName">Name of the property in the object to index</param>
        /// <returns>True if the indexing has completed successfully</returns>
        public bool IndexString(string objectName, string propertyName) {
            return IndexString(objectName, propertyName, BuildStringList(objectName, propertyName));
        }
        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>
        ///     Indexes a specific String field of an object in the database.
        ///     The index data is provided in the data parameter.
        /// </summary>
        /// <param name="objectName">Name of the object to index</param>
        /// <param name="propertyName">Name of the property in the object to index</param>
        /// <param name="data">The data used to build the index.  The keys in the KeyValuePairs and the values contain the textual information.</param>
        /// <returns>True if the indexing has completed successfully</returns>
        public bool IndexString(string objectName, string propertyName, List<KeyValuePair<uint, string>> data) {
            bool success = false;

            //-----b1----- Generate the stub of the set name for our text sets
            // e.g. MyObject:i:MyProperty:aa or HashTagStart + objectName + IndexNumeric + propertyName + HashTagEnd + ":"
            string setNameStub = KeyName(objectName, propertyName, SearchType.Text);

            //-----b2----- Instantiate our chunk parameters
            List<Task<bool>> tasks = new List<Task<bool>>();
            int totalCount = data.Count;
            int counter = 0;
            int chunkSize = RedisWrapper.WriteChunkSize;
            int startIndex = 0;

            //-----c----- Iterate through the chunks
            while (counter < totalCount) {

                //-----c1----- Catch the cases where the number of results is less than our chunkSize
                chunkSize = ((startIndex + chunkSize) <= totalCount) ? chunkSize : (totalCount - startIndex);
                Logger.Log("Processing a " + chunkSize + " object chunk, starting at index " + startIndex + ".");

                //-----c2----- Iterate through the objects in this chunk and add them to a set asyncronously ...
                for (int i = startIndex; i < startIndex + chunkSize; i++) {
                    KeyValuePair<uint, string> kvp = data[i];

                    //-----c3----- Special case with the text entries is if the given value is null or too short.  In this case, we need to pad it with some special chars ...
                    string keyStub = null;
                    if (kvp.Value == null || kvp.Value.Length < TextIndexLength) {
                        keyStub = (kvp.Value == null) ? "" : kvp.Value;
                        while( keyStub.Length < TextIndexLength) {
                            keyStub = keyStub + "_";
                        }
                    } else {
                        keyStub = kvp.Value.Substring(0, TextIndexLength).ToLower();
                    }

                    tasks.Add(rDBs[CurrentConnection].SetAddAsync(setNameStub + keyStub, kvp.Key));

                    Logger.Log(++counter, 1000, data.Count);
                }
                Logger.Log("");

                //-----d----- Check it all ran successfully in a soft manner using the AwaitTasks approach
                success = success & AwaitTasks(tasks, "indexing property " + propertyName + " for object " + objectName + ".");

                //-----e1----- Reset and garbage collect
                tasks = new List<Task<bool>>();
                GC.Collect();

                //-----e2----- Pause if there is a decent wodge of data
                if (totalCount > RedisWrapper.WriteIndexPauseThreshold) {
                    Logger.Log("Pausing for a breather...");
                    Thread.Sleep(RedisWrapper.WriteIndexPause);
                }

                //-----e3----- Set for the next chunk....
                startIndex += chunkSize;
            }


            //-----f----- Before leaving the method, lets kill what we can and call garbage collection to minimise the memory usage if possible
            tasks = null;
            GC.Collect();

            return success;
        }


        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>
        ///     Builds a list of data relating to the IDs and the specified text property of our object.  The data is extracted from the objects
        ///     that have already been written in the database.
        ///     Note for testing, see also RedisTestObj.BuildStringList
        /// </summary>
        /// <param name="objectName">Name of the object to index</param>
        /// <param name="propertyName">Name of the property in the object to index</param>
        /// <returns>A list of KeyValue pairs containing the ID and text value of the requested property.</returns>
        public List<KeyValuePair<uint, string>> BuildStringList(string objectName, string propertyName) {
            // Our return value
            List<KeyValuePair<uint, string>> data = new List<KeyValuePair<uint, string>>();

            //-----a1----- Task 1 - Lets check that there is a primary key index for this object
            string pk = KeyName( objectName );

            Task<bool> task1 = rDBs[CurrentConnection].KeyExistsAsync(pk);
            if (task1.Result == false) {
                Logger.LogError(6, "RedisWrapper.BuildStringList - the primary key does not exist for object " + objectName + ".  Cannot continue with the indexing without this.");
                return null;
            }

            //-----a1----- Task 2 - Lets check that the primary key index for this object contains some results!
            Task<long> task2 = rDBs[CurrentConnection].SetLengthAsync(pk);
            long totalObjects = task2.Result;
            if (totalObjects == 0) {
                Logger.LogWarning("RedisWrapper.BuildStringList - the primary key contains no objects " + objectName + ".  No point continuing!");
                return null;
            }


            //-----b----- So now iterate through all the objects and extract the info into a dictionary, then write a list of KeyValuePairs for that info with the ID of each object....
            List<KeyValuePair<uint, Task<RedisValue>>> tasks = new List<KeyValuePair<uint, Task<RedisValue>>>();
            // SetMembersAsync is much faster than SetScan...
            Task<RedisValue[]> task3 = rDBs[CurrentConnection].SetMembersAsync(pk, CommandFlags.None);
            // So now iterate through all the results
            foreach (RedisValue rk in task3.Result) {

                //-----b1----- Parse our IDs as uints
                uint id = ConvertID(rk);

                //-----b2----- Now get the hash name
                string hashName = KeyName( objectName, + id);

                //-----b3----- And then get the actual value we want to index here.  Async of course so then we need to store the results of this in another list of tasks
                tasks.Add(new KeyValuePair<uint, Task<RedisValue>>(id, rDBs[CurrentConnection].HashGetAsync(hashName, propertyName)));
            }

            //-----c----- Now go through the async values and get the results ...
            foreach (KeyValuePair<uint, Task<RedisValue>> t in tasks) {
                if (t.Value.IsFaulted == true) {
                    Logger.LogWarning("RedisWrapper.BuildStringList - could not extract the value for task ID " + t.Value.Id + " - the given error message was " + t.Value.Exception.ToString());
                } else {
                    // Lets get our string value
                    string indexVal = t.Value.Result;
                    // And lets add the ID and the string value to our list of KVPs
                    data.Add(new KeyValuePair<uint, string>(t.Key, indexVal));
                }
            }

            return data;
        }


        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>
        ///     Builds a list of data relating to the IDs and the specified numeric property of our object.  The data is extracted from the objects
        ///     that have already been written in the database, either from the individual Hashes or the pregenerated index.
        ///     Note for testing, see also RedisTestObj.BuildNumericList
        /// </summary>
        /// <param name="objectName">Name of the object to index</param>
        /// <param name="propertyName">Name of the property in the object to index</param>
        /// <param name="loadFromIndex">Whether or not to load the numeric information from the index or from the individual hashes</param>
        /// <returns>A list of KeyValue pairs containing the ID and numeric value of the requested property.</returns>
        public List<KeyValuePair<uint, double>> BuildNumericList(string objectName, string propertyName, bool loadFromIndex) {
            // Our return data
            List<KeyValuePair<uint, double>> data = new List<KeyValuePair<uint, double>>();

            if (loadFromIndex == false) {

                //-----a1----- Task 1 - Lets check that there is a primary key index for this object
                string pk = KeyName(objectName);

                Task<bool> task1 = rDBs[CurrentConnection].KeyExistsAsync(pk);
                if (task1.Result == false) {
                    Logger.LogError(6, "The primary key does not exist for object " + objectName + ".  Cannot continue with the indexing without this.");
                    return null;
                }

                //-----a1----- Task 2 - Lets check that the primary key index for this object contains some results!
                Task<long> task2 = rDBs[CurrentConnection].SetLengthAsync(pk);
                long totalObjects = task2.Result;
                if (totalObjects == 0) {
                    Logger.LogWarning("The primary key contains no objects " + objectName + ".  No point continuing!");
                    return null;
                }

                //-----b----- So now iterate through all the objects and extract the info into a dictionary, then write a list of KeyValuePairs for that info with the ID of each object....
                List<KeyValuePair<uint, Task<RedisValue>>> tasks = new List<KeyValuePair<uint, Task<RedisValue>>>();
                // SetMembersAsync is much faster than SetScan...
                Task<RedisValue[]> task3 = rDBs[CurrentConnection].SetMembersAsync(pk, CommandFlags.None);
                // So now iterate through all the results
                foreach (RedisValue rk in task3.Result) {

                    //-----b1----- Parse our IDs as uints
                    uint id = ConvertID(rk);

                    //-----b2----- Now get the hash name
                    string hashName = KeyName(objectName, id);

                    //-----b3----- And then get the actual value we want to index here.  Async of course so then we need to store the results of this in another list of tasks
                    tasks.Add(new KeyValuePair<uint, Task<RedisValue>>(id, rDBs[CurrentConnection].HashGetAsync(hashName, propertyName)));
                }

                //-----c----- Now go through the async values and get the results ...
                foreach (KeyValuePair<uint, Task<RedisValue>> t in tasks) {
                    if (t.Value.IsFaulted == true) {
                        Logger.LogWarning("RedisWrapper.BuildNumericList - could not extract the value for task ID " + t.Value.Id + " - the given error message was " + t.Value.Exception.ToString());
                    } else {

                        // Parse the RedisValue
                        double indexVal = 0;
                        double.TryParse(t.Value.Result, out indexVal);

                        // And lets add the ID and the numeric value to our list of KVPs
                        data.Add(new KeyValuePair<uint, double>(t.Key, indexVal));
                    }
                }

            } else { 

                //------d----- Ok, this is the easier approach - we can just load our data directly from the pregenerated index ...
                foreach (SortedSetEntry sse in rDBs[CurrentConnection].SortedSetScan(KeyName(objectName, propertyName, SearchType.Score), "*", 1000)) {
                    // Get the ID
                    uint id = ConvertID(sse.Element);
                    // And add our data
                    data.Add(new KeyValuePair<uint, double>(id, sse.Score));
                }
            }
            
            return data;
        }


        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>
        ///     Generates the key name of the primary key index for the given objectName
        /// </summary>
        /// <param name="objectName">Name of the object to index</param>
        public string KeyName(string objectName) {
            return KeyName(objectName, "ID", SearchType.PrimaryKey, "");
        }
        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>
        ///     Generates the key name of the hash containing the full data for the given object for the given object ID
        /// </summary>
        /// <param name="objectName">Name of the object to index</param>
        /// <param name="objID">The ID of the specific object whose hash key name will be generated</param>
        public string KeyName(string objectName, uint objID) {
            return objectName + ":" + objID;
        }
        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>
        ///     Generates the key name for the index relating to the given property name for the given objectName
        /// </summary>
        /// <param name="objectName">Name of the object to index</param>
        /// <param name="propertyName">Name of the property in the object to index</param>
        /// <param name="searchType">The type of search - a generalised view of the property type.</param>
        public string KeyName(string objectName, string propertyName, SearchType searchType) {
            return KeyName(objectName, propertyName, searchType, "");
        }
        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>
        ///     Generates the key name for the index relating to the given property name for the given objectName
        /// </summary>
        /// <param name="objectName">Name of the object to index</param>
        /// <param name="propertyName">Name of the property in the object to index</param>
        /// <param name="searchType">The type of search - a generalised view of the property type.</param>
        /// <param name="textOrBoolSet">The specific set for which to generate the keyName.  Bool and Text data is indexed in sets e.g. 0 or 1 and aa or bz etc.</param>
        public string KeyName(string objectName, string propertyName, SearchType searchType, string textOrBoolSet) {

            string keyName = "";

            // So the only thing to mention here is that all the indexes using HashTags apart from the primary key index.
            // Perhaps we should standardise this as searching for keys with a hashtags currently is an easy way to pull out the other indexes.
            if (searchType == SearchType.PrimaryKey) {
                keyName = objectName + IndexPrimaryKeySuffix;

            } else if (searchType == SearchType.Score) {
                keyName = HashTagStart + objectName + IndexNumeric + propertyName + HashTagEnd;

            } else if (searchType == SearchType.Text) {
                keyName = HashTagStart + objectName + IndexText + propertyName + HashTagEnd + ":" + textOrBoolSet; 

            } else if (searchType == SearchType.Bool) {
                keyName = HashTagStart + objectName + IndexText + propertyName + HashTagEnd + ":" + textOrBoolSet;

            } else if (searchType == SearchType.DateTime) {
                keyName = HashTagStart + objectName + IndexText + propertyName + HashTagEnd;

            }

            return keyName;
        }

        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>
        ///     Checks whether or not an index exists.
        /// </summary>
        /// <param name="objectName">Name of the object</param>
        /// <param name="propertyName">Name of the property in the object</param>
        /// <param name="searchType">The type of search - a generalised view of the property type.</param>
        /// <returns>True if the index is present in the Redis database instance.</returns>
        public bool IndexExists(string objectName, string propertyName, SearchType searchType) {
            bool exists = false;

            if (string.IsNullOrEmpty(objectName) == false && string.IsNullOrEmpty(propertyName) == false) {
                string textSet = "";
                if (searchType == SearchType.Text) {
                    // We just search for the aa index ...
                    for (int i = 0; i < TextIndexLength; i++) {
                        textSet = textSet + "a";
                    }
                } else if (searchType == SearchType.Bool) {
                    // We search for the "false" set
                    textSet = "0";
                }

                string keyName = KeyName(objectName, propertyName, searchType, textSet);
                exists = rDBs[CurrentConnection].KeyExists(keyName);
            }

            return exists;
        }

        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>
        ///     The Redis index info is useful for query optimisation as we use this info to sort a list of SearchPatterns
        /// </summary>
        /// <param name="obj"></param>
        /// <returns></returns>
        public List<DatabaseIndexInfo> IndexFindAll(object obj) {
            List<DatabaseIndexInfo> riis = new List<DatabaseIndexInfo>();

            // Get the object name and properties ...
            string objName = null;
            PropertyInfo[] objPI = null;
            GetObjectProperties(obj, out objName, out objPI);

            // Loop through these properties and see if an index exists for it ...
            foreach( PropertyInfo pi in objPI) {
                DatabaseIndexInfo rii = new DatabaseIndexInfo();
                rii.IndexName = pi.Name;
                rii.IndexType = pi.PropertyType.Name;

                // Special case for the primary key ....
                if (pi.Name == "ID") {
                    rii.SearchType = SearchType.PrimaryKey;
                } else {
                    if (pi.PropertyType.Name == "Int64") {
                        rii.SearchType = SearchType.Score;
                    } else if (pi.PropertyType.Name == "UInt64") {
                        rii.SearchType = SearchType.Score;
                    } else if (pi.PropertyType.Name == "UInt32") {
                        rii.SearchType = SearchType.Score;
                    } else if (pi.PropertyType.Name == "Int32") {
                        rii.SearchType = SearchType.Score;
                    } else if (pi.PropertyType.Name == "Double") {
                        rii.SearchType = SearchType.Score;
                    } else if (pi.PropertyType.Name == "String") {
                        rii.SearchType = SearchType.Text;
                    } else if (pi.PropertyType.Name == "DateTime") {
                        rii.SearchType = SearchType.DateTime;
                    } else if (pi.PropertyType.Name == "Boolean") {
                        rii.SearchType = SearchType.Bool;
                    }
                }

                // Now lets see if that index exists ...
                if ( IndexExists(objName, rii.IndexName, rii.SearchType) == true) {

                    // right get the max and min if this is a date time or a score!!
                    if ( rii.SearchType == SearchType.Score || rii.SearchType == SearchType.DateTime) {
                        // build the keyName
                        string keyName = KeyName(objName, rii.IndexName, rii.SearchType);
                        // get the cardinality
                        long cardinality = rDBs[CurrentConnection].SortedSetLength(keyName);
                        // so now just get the first and last score
                        RedisValue[] firstScore = rDBs[CurrentConnection].SortedSetRangeByRank(keyName, 0, 0);
                        RedisValue[] lastScore = rDBs[CurrentConnection].SortedSetRangeByRank(keyName, cardinality - 1, cardinality - 1);
                        // and now lets set it!
                        rii.MinMax = new KeyValuePair<double, double>(
                            (double)rDBs[CurrentConnection].SortedSetScore(keyName,firstScore[0]), 
                            (double)rDBs[CurrentConnection].SortedSetScore(keyName, lastScore[0]));
                    }

                    riis.Add(rii);
                }

            }


            return riis;
        }



        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>
        ///     Searches the values stored for a particular type of object.
        ///     Note that any pattern grouping containing a PrimaryKey search type will automatically be an intersection, irrespective of the value of doIntersect
        /// </summary>
        /// <param name="objectName">The name of the object to extract from the database</param>
        /// <param name="patterns">
        ///     A list of propertyNames (the keys) and the patterns to search for in each of these properties (the values)
        /// </param>
        /// <param name="doIntersect">
        ///     If true, the intersection between the patterns provided will be checked, otherwise the union will be returned.  
        ///     Not that this is equivalent to "and" and "or" respectively in a traditional RDBMS
        /// </param>
        public async Task<List<uint>> SearchObjects(string objectName, List<DatabaseSearchPattern> patterns, bool doIntersect) { //, bool blah) {
            return await SearchObjects(objectName, patterns, doIntersect, null);
        }
        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        public async Task<List<uint>> SearchObjects(string objectName, List<DatabaseSearchPattern> patterns, bool doIntersect, List<DatabaseIndexInfo> riis) { //, bool blah) {

            try {

                //------a------ Sort the Search Patterns into the likely optimal processing orientation ...
                patterns.Sort(DatabaseSearchPattern.Sort(riis));

                //-----b----- So now they are sorted, the PK should always be the first pattern if the sorting is working correctly ...            
                bool patternsTestPrimaryKey = (patterns[0].SearchType == SearchType.PrimaryKey);
                //-----c1----- If there is a primary key clause, lets optimise and just search our specific object ...
                if (patternsTestPrimaryKey == true) {

                    // Warn if SearchObjects was called for a union, but a primary key clause has been detected ...
                    if (doIntersect == false) {
                        Logger.LogWarning("RedisWrapper.SearchObjects - a primary key search type has been identified in the given list of patterns.  This will automatically override the doIntersect clause, which you have set to union.");
                        Logger.Log("The specific search pattern(s) are: ");
                        foreach (DatabaseSearchPattern p in patterns) {
                            Logger.Log(p.AsText());
                        }
                    }

                    //-----b2----- Run the match that is optimised if a primary key is available and return immediately to try to squeeze out the speed ...
                    if (DoAsync == true) {
                        return await MatchObject(objectName, patterns);
                    } else {
                        return MatchObject(objectName, patterns).Result;
                    }


                } else if (patterns.Count == 1) {           //-----c2----- If there is only one pattern, we only need to call search objects worker method directly once

                    //-----c2----- If there is only one pattern, that is a RESULT - should be relatively fast.  Lets help it on its way by running this here as a specific option ..
                    // Return immediately to try to squeeze out the speed
                    if (DoAsync == true) {
                        return await SearchObjects(objectName, patterns[0]);
                    } else {
                        return SearchObjects(objectName, patterns[0]).Result;
                    }


                } else {

                    /*
                     *      -----d------ The bucket case - we need to process multiple search queries and find the intersection or union between them
                     *      The different criteria have been sorted, hopefully with the most optimal first, so once we have the results from that, we can
                     *      then determine if we should run a full search using the second, third, fourth criteria etc or just do a range-scan approach
                     *      where we iterate through the results from the first range query and then scan the relevant attributes to the following parameters
                     *      in each of the corresponding objects.
                    */

                    List<uint> listOfIDs = new List<uint>();

                    //-----d1----- Get the first list and use the count from this list to determine whether to range scan or do the full query for the remaining search patterns ....
                    if (DoAsync == true) {
                        listOfIDs = await SearchObjects(objectName, patterns[0]);
                    } else {
                        listOfIDs = SearchObjects(objectName, patterns[0]).Result;
                    }


                    //-----d2----- Special case for booleans - we always want to process these as Range Scans as the sets are likely to be 50% true / false and so will pull out loads of results.
                    bool containsABoolSearch = false;
                    foreach (DatabaseSearchPattern rsp in patterns) {
                        if (rsp.SearchType == SearchType.Bool) {
                            containsABoolSearch = true;
                            break;
                        }
                    }

                    //-----d3----- Return immediately if the empty array if the first search drew a blank ....
                    if (listOfIDs.Count == 0) {
                        return listOfIDs;

                    } else if (doIntersect == true && DoRangeScan == true &&
                        (listOfIDs.Count < RangeScanThreshold || containsABoolSearch == true)) {   //-----d4----- Otherwise scan if the number of results is relatively small OR there is a bool query

                        // Right - first things first, we want to clone the List of patterns so we dont mess up the real one ...
                        //***** look at possibly doing this in the loop due to the await clause...
                        List<DatabaseSearchPattern> tempRSPs = new List<DatabaseSearchPattern>();
                        foreach(DatabaseSearchPattern rsp in patterns) {
                            tempRSPs.Add(rsp);
                        }

                        // Now lets use MatchObjects - which requires a primary key clause, so we replace the first search pattern with the PK for each of the results found relating
                        // to the first search parameter.  So we can just replace the first search param (which is calculated above) with the PK clause for each ID in a loop.
                        List<uint> objIDsMatching = new List<uint>();

                        foreach (uint id in listOfIDs) {

                            // Reset the first search pattern to be our PK clause ...
                            tempRSPs[0] = new DatabaseSearchPattern(id);

                            // Really we should only ever have one result here
                            objIDsMatching.AddRange( await MatchObject(objectName, tempRSPs));
                        }

                        // And lastly, lets now return the temp objs list that are matching our pattern
                        return objIDsMatching;

                    } else {

                        //-----e----- Ok - this really really is the bucket case - and we will now have to do intersections and unions etc so will need the slightly heavier HashSets
                        // (much slower to write but blindingly fast to computer IntersectWith and UnionWIth)
                        List<List<uint>> listOfListsOfIDs = new List<List<uint>>();

                        listOfListsOfIDs.Add(listOfIDs);

                        //-----e1----- Iterate through the patterns provided and lets test each one           
                        for (int i = 1; i < patterns.Count; i++) {

                            DatabaseSearchPattern pattern = patterns[i];
                            //List<uint> tempIDs = null;
                            /////////////////////////////////////////////////////////////////
                            // There is something weird going on in here with the object IDs being overwritten before it has a chance to move accross ...

                            //-----e2----- The default case - fire off another search query to try to narrow things down ..
                            if (DoAsync == true) {
                                listOfListsOfIDs.Add(await SearchObjects(objectName, pattern));
                            } else {
                                listOfListsOfIDs.Add(SearchObjects(objectName, pattern).Result);
                            }

                            //-----e3----- If this is an intersection and there are no objects returned, then quit immediately as there will not EVER be an intersection!!
                            if (listOfListsOfIDs[listOfListsOfIDs.Count - 1].Count == 0 && doIntersect == true) {
                                // 21-Jul-2016 - Um, so if this is the e.g. second or third iteration (i.e. not the first), then there may already be IDs loaded in the objs list
                                // so we should clear these first
                                listOfListsOfIDs.Clear();
                                break;
                            }

                        }

                        //-----e4----- Now finally if we have results then lets combine the lists as an intersection or union
                        listOfIDs = DataStructures.CombineLists(listOfListsOfIDs, doIntersect);

                        //-----e5----- And lets finally return our list ..
                        return listOfIDs;
                    }
                }


            } catch (Exception ex) {
                // Errors have occurred - so lets try to provide the necessary info ...
                StringBuilder queryTxt = new StringBuilder();
                foreach (DatabaseSearchPattern rsp in patterns) {
                    queryTxt.Append(" " + rsp.AsText());
                }

                Logger.LogError(7, "SearchObjects - an error occurred with trying to confirm the existence and attribution of the object matching these patterns: "
                    + queryTxt + ".  The specific error was:\n" + ex.ToString());
            }

            // We will only get to here if there has been an error in the above ...
            return null;
        }



        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        public static void DebugStart(int taskID, List<DatabaseSearchPattern> rsps, List<DatabaseIndexInfo> riis) {

            if (RedisWrapper.DoDebug == true) {

                // Sort the results - so that the picture is clearer ....
                // 19-Aug-2016 - Fuck a duck - DO NOT SORT THE QUERIES HERE
                // We need to make sure not to sort the master list as it potentially confuses the calculations in the search objects method ...
                //List<RedisSearchPattern> tempRSPs = new List<RedisSearchPattern>();
                //foreach( RedisSearchPattern rsp in rsps) {
                //    tempRSPs.Add(rsp);
                //}
                //tempRSPs.Sort(RedisSearchPattern.Sort(riis));

                lock (Debug) {
                    RedisWrapper.Debug.Add(new DatabaseDebug(taskID, DateTime.Now, rsps));
                }
                
            }            
        }


        ////--------------------------------------------------------------------------------------------------------------------------------------------------------------
        private static void DebugFinish(int taskID, bool isFaulted, int resultCount) {

            if (DoDebug == true) {
                bool foundTask = false;
                /////////////////////////////////////////
                for( int i = 0; i < Debug.Count;  i++ ) {
                    if ( Debug[i].TaskID == taskID) {
                        lock(Debug) {
                            Debug[i].Duration = DateTime.Now.Subtract(Debug[i].Start).TotalMilliseconds;
                            Debug[i].ResultCount = resultCount;

                            StringBuilder queryTxt = new StringBuilder();
                            foreach (DatabaseSearchPattern rsp in Debug[i].SearchPatterns) {
                                queryTxt.Append(" " + rsp.AsText());
                            }

                            Debug[i].IsFaulted = isFaulted;

                            if ( isFaulted == true ) {
                                Debug[i].SearchPatternsAsText = "FAULTED so found zero results for " + queryTxt.ToString();                                
                            } else {
                                Debug[i].SearchPatternsAsText = "Found " + resultCount + " results for " + queryTxt.ToString();
                            }
                            
                        }
                        foundTask = true;
                        break;
                    }
                }

                if ( foundTask == false) {
                    Logger.LogWarning("Could not find task ID: "+taskID+".  Something odd is going on.");
                }
            }
        }


        //public RedisValue[] GetID( long id ) {
        //    return new RedisValue[] { id };
        //}

        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>
        ///     Searches the values stored for a particular type of object.
        ///     override this to pull out specific IDs faster
        ///     override this to pull out one specfic parameter faster
        /// </summary>
        /// <param name="objectName">The name of the object to extract from the database</param>
        /// <param name="patterns">
        ///     A list of propertyNames (the keys) and the patterns to search for in each of these properties (the values)
        /// </param>
        /// <param name="doIntersect">
        ///     If true, the intersection between the patterns provided will be checked, otherwise the union will be returned.  
        ///     Not that this is equivalent to "and" and "or" respectively in a traditional RDBMS
        /// </param>
        //        public List<long> SearchObjects(string objectName, RedisSearchPattern pattern) {
        //            List<long> objs = new List<long>();
        //public Task<RedisValue[]> SearchObjects(string objectName, RedisSearchPattern pattern) {
        //        public async Task<List<RedisValue>> SearchObjects(string objectName, RedisSearchPattern pattern) {
        public async Task<List<uint>> SearchObjects(string objectName, DatabaseSearchPattern pattern) {
            List<uint> objs = new List<uint>();

            try {

                Task<RedisValue[]> task = null;

                //-----a----- e.g. MyObject:Index:MyProperty or MyObject:IndexText:MyProperty:Some Text 
                // HashTagStart + objectName + IndexNumeric + pattern.Parameter + HashTagEnd;
                string indexName = KeyName(objectName, pattern.Parameter, pattern.SearchType);


                //-----b----- If this data item is indexed, lets use the indexes!  Otherwise we need to scan through all the objects...
                if (pattern.SearchType == SearchType.PrimaryKey) {
                    // Just add the primary key automatically ---
                    // Its the ids, so we can cast this.  We can't really change the score as this _could_ be negative ...

                    // Check this is working ok....
                    objs.Add(ConvertID(pattern.Score));

                } else if (pattern.SearchType == SearchType.Text) {
                    /*
                     *  -----c-----
                     *  So here we want to mimic a database and make this search case independent .... But Redis is not a fucking database - the text searches are binary implementations
                     *  Note that the string comparisons are conducted as a binary array of bytes (http://redis.io/commands/zrangebylex and https://en.wikipedia.org/wiki/UTF-8).  
                     *  This means that the first character has the most importance.  Note also that uppercase letters precede lower case letters
                     *  So here we want to do both upper and lower case searches (obvs for nums and chars this is not needed!)
                     *  Also, to mimic the * wildcard we need to search up until the next character (e.g. >= a and < b)... there are of course some wiggly special cases here 
                     *  To get all z's you need to search until { and to get all Z's you need to search until [, because these chars are ordered as UTF-8 (see http://www.fileformat.info/info/charset/UTF-8/list.htm )
                    */

                    // and lets go back to ranges!!! Umm no - about four times slower!
                    //string sortedSetName = HashTagStart + objectName + IndexText + pattern.Parameter + HashTagEnd;
                    //task = rDBs[CurrentConnection].SortedSetRangeByValueAsync(sortedSetName, pattern.PatternMin, pattern.PatternMax);

                    // Lets get the pattern to search for - remember that the updated text searches are optimised to just use sets of IDs with the starts with text substring as the key
                    string setName = KeyName(objectName, pattern.Parameter, pattern.SearchType, pattern.PatternStartsWith(RedisWrapper.TextIndexLength));
                    //HashTagStart + objectName + IndexText + pattern.Parameter + HashTagEnd + ":" + pattern.PatternStartsWith;

                    // 11-Aug-2016 - Lets try using set Scan instead ... Umm no - SetMembersAsync is about twice as fast!
                    task = rDBs[CurrentConnection].SetMembersAsync(setName);
                    ////                //foreach (RedisValue rv in rDBs[CurrentConnection].SetScan(setName, "*", 1000, CommandFlags.None)) {
                    //////                    objs.Add(ConvertID(rv));
                    //////                }


                } else if (pattern.SearchType == SearchType.Bool) { // DEPRECATED as ridiculously slow

                    Logger.LogWarning("Searching using boolean values is deprecated as it is very very slow in Redis - Redis works best for finding the needle in the haystack!");

                    string setName = KeyName(objectName, pattern.Parameter, pattern.SearchType, pattern.Score.ToString());
                    //string setName = HashTagStart + objectName + IndexNumeric + pattern.Parameter + HashTagEnd + ":" + pattern.Score;

                    // 11-Aug-2016 - Lets try using set Scan instead ... Umm no - SetMembersAsync is about twice as fast!
                    task = rDBs[CurrentConnection].SetMembersAsync(setName, CommandFlags.None);


                } else if (pattern.SearchType == SearchType.DateTime) {

                    // Back to using ranges as faster than using the blocks and gives us more flexibility...
                    string sortedSetName = KeyName(objectName, pattern.Parameter, pattern.SearchType);
                    //long baseTicks = RedisWrapper.DateBase.Ticks;
                    //long min = (pattern.ScoreMin < baseTicks) ? 0 : (long)(pattern.ScoreMin - RedisWrapper.DateBase.Ticks) / 10000000;
                    //long min = (pattern.ScoreMin == DateTime.MaxValue.Ticks) ? -1 : (long)(pattern.ScoreMin - RedisWrapper.DateBase.Ticks) / 10000000;
                    //long max = (pattern.ScoreMax == DateTime.MaxValue.Ticks) ? -1 : (long)(pattern.ScoreMax - RedisWrapper.DateBase.Ticks) / 10000000;

                    //if (pattern.SearchComparisonExpression == DNSearchComparisonExpression.RangeGreaterThanOrEqualTo) {

                    //} else if (pattern.SearchComparisonExpression == DNSearchComparisonExpression.RangeGreaterThanOrEqualTo) {

                    //} else {  // Between or equivalent ...

                    //}

                    //HashTagStart + objectName + IndexNumeric + pattern.Parameter + HashTagEnd;
                    //task = rDBs[CurrentConnection].SortedSetRangeByScoreAsync(sortedSetName, min, max);
                    task = rDBs[CurrentConnection].SortedSetRangeByScoreAsync(sortedSetName, pattern.ScoreMin, pattern.ScoreMax);

                } else if (pattern.SearchType == SearchType.Score) {

                    // Very simply - we just get the range from the sorted set ...
                    task = rDBs[CurrentConnection].SortedSetRangeByScoreAsync(indexName, pattern.ScoreMin, pattern.ScoreMax);

                }
                //                } else {
                // No index so we need to scan through all the objects here ...
                //                    task = ScanObjects(objectName, pattern);

                //                }
                // NO SCANNING - Therefore we need to enable a "Index available" or exists method!!!

                //}


                if (task != null) {
                    if (DoAsync == true) {
                        await task;
                    }

                    foreach (RedisValue rv in task.Result) {
                        objs.Add(ConvertID(rv));
                    }
                }

            } catch (Exception ex) {
                // Errors have occurred - so lets try to provide the necessary info ...
                Logger.LogError(7, "SearchObjects - an error occurred with trying to confirm the existence and attribution of the object matching these patterns: "
                    + pattern.AsText() + ".  The specific error was:\n" + ex.ToString());
            }


            return objs;

        }


        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>
        ///    Similar to the search objects code, but optimised for one of the patterns provided being a primary key search
        ///    So we extract the object rather than doing the necessary range queries ...
        ///    For the primary key searches, doing a Union is totally irrelevant nah????
        /// </summary>
        public async Task<List<uint>> MatchObject(string objectName, List<DatabaseSearchPattern> patterns) { //, bool doIntersect) {
            List<uint> objs = new List<uint>();
            //List<RedisValue> objs = new List<RedisValue>();

            try {

                // right first things first, lets get the PK
                // 11-Aug-2016 - note that the search patterns should now be sorted ...
                if (patterns[0].SearchType != SearchType.PrimaryKey) {
                    Logger.LogWarning("MatchObject - Could not find a primary key search in the first pattern listed, so defaulting to the more generic SearchObjects algorithm.");
                    // didn't find a PrimaryKey searchType, so lets return the more generic search objects query ....
                    ////////////////////////////////////////////////////////////////////
                    return await SearchObjects(objectName, patterns, true);
                }

                uint pkID = ConvertID(patterns[0].Score);

                bool matches = false;


                /////////////////////////////////////////
                // ok here we probably need to do something a bit more ....
                // We should anyway do the PK match ... in fact it is implicit as we wounldnot be able to get the values if the PK did not exist!!!

                // OK, now if there is just one pattern - it is JUST the PK, so lets just go
                // if 2 patterns, then lets extract the value; otherwise we get the whole object list ....
                if (patterns.Count == 1) {

                    // Rather than just throwing the PK back at the user, we should actually check whether or not it exists!!!
                    matches = await rDBs[CurrentConnection].KeyExistsAsync(KeyName(objectName, pkID)); // objectName + ":" + pkID);

                } else if (patterns.Count > 1) {

                    if (patterns.Count == 2) {

                        // get the name of the other field
                        //RedisSearchPattern otherPattern = (patterns[0].SearchType == DNSearchType.PrimaryKey) ? patterns[1] : patterns[0];
                        DatabaseSearchPattern otherPattern = patterns[1];

                        // objectName + ":" + pkID
                        RedisValue rv = await rDBs[CurrentConnection].HashGetAsync(KeyName(objectName, pkID), otherPattern.Parameter);

                        matches = ObjectPropertyMatches(otherPattern, rv);

                    } else {

                        // Multiple parameters to test, so lets get the HashSet and check each of the parameters ...
                        List<RedisValue> fieldsToExtract = new List<RedisValue>();
                        for (int i = 1; i < patterns.Count; i++) {
                            fieldsToExtract.Add(patterns[i].Parameter);
                        }
                        //RedisValue[] hes = await GetMultipleAttributes(pkID, objectName, fieldsToExtract);
                        RedisValue[] hes = await GetMultipleAttributes(pkID, objectName, fieldsToExtract.ToArray());
                        //HashEntry[] hes = await GetObjectStart(pkID, objectName);


                        if (hes != null && hes.Length > 1) {
                            int counter = 0;
                            foreach (DatabaseSearchPattern rsp in patterns) {
                                bool tempMatches = true; // innocent until proven guilty ...

                                if (rsp.SearchType != SearchType.PrimaryKey) {
                                    //foreach (HashEntry he in hes) {
                                    //                                    if (rsp.Parameter.Equals(he.Name, StringComparison.CurrentCultureIgnoreCase) == true) {
                                    //tempMatches = ObjectPropertyMatches(rsp, he.Value);
                                    //                                        break;
                                    //                                    }
                                    //                                }
                                    tempMatches = ObjectPropertyMatches(rsp, hes[counter++]);
                                    //counter++;
                                }

                                if (tempMatches == false) {
                                    // This is an intersection, so lets get out of here!!!
                                    matches = false;
                                    break;
                                } else {
                                    matches = true;
                                }
                            }
                        } else {
                            // Lets log if there are issues here ...
                            StringBuilder txt = new StringBuilder();
                            foreach (RedisValue rv in fieldsToExtract) {
                                if (txt.Length > 0) {
                                    txt.Append(", ");
                                }
                                txt.Append(rv);
                            }

                            Logger.LogWarning("Possible issue with GetMultipleAttributes for ID: " + pkID + " when extracting these fields from the Hash: " + txt);
                        }
                    }
                }

                // Add the object if it still matches ...
                if (matches == true) {
                    objs.Add(pkID);
                }

            } catch( Exception ex) {
                StringBuilder queryTxt = new StringBuilder();
                foreach (DatabaseSearchPattern rsp in patterns) {
                    queryTxt.Append(" " + rsp.AsText());
                }

                Logger.LogError(7, "MatchObject - an error occurred with trying to confirm the existence and attribution of the object matching these patterns: "
                    + queryTxt + ".  The specific error was:\n" + ex.ToString());
            }

            return objs;
        }

        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>
        ///         Get an object from the database given an ID
        /// </summary>
        public Task<RedisValue[]> GetMultipleAttributes(uint id, string objectName, RedisValue[] hashFields) {

            try {
                // and then get the data from the db here
                //string hashName = KeyName(objectName, id); // objectName + ":" + id;
                return rDBs[CurrentConnection].HashGetAsync(KeyName(objectName, id), hashFields);
            } catch (Exception ex) {
                string ohbugger = "";
            }

            return null;
        }


        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>
        ///     Removes IDs from the given HashSet whose values don't match the given pattern
        /// </summary>
        public async Task<List<uint>> ScanObjects(string objectName, List<uint> ids, DatabaseSearchPattern pattern) {
            List<uint> objIDs = new List<uint>();

            //List<RedisValue> tempRVs = new List<RedisValue>();
            foreach (uint id in ids) {

                RedisValue tempRV = "";

                if (DoAsync == true) {
                    tempRV = await rDBs[CurrentConnection].HashGetAsync(KeyName(objectName, id), pattern.Parameter);
                    //objectName + ":" + id, pattern.Parameter);
                } else {
                    tempRV = rDBs[CurrentConnection].HashGetAsync(KeyName(objectName, id), pattern.Parameter).Result;
                }

                // If we could not find the object, then lets remove it ...
                if (ObjectPropertyMatches(pattern, tempRV) == true) {
                    //ids.Remove(id);
                    //tempObjs.Add(id);
                    objIDs.Add(id);
                }
            }

            // Hmmmm need a success metric here ... perhaps a try / catch????

            return objIDs;
            //return success;
        }


        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>
        /// /////////////////////////////////////////////////////////////////
        /// </summary>
        public bool ObjectPropertyMatches(DatabaseSearchPattern pattern, RedisValue rv) {

            bool matches = false;

            if (pattern.SearchType == SearchType.PrimaryKey) {
                // Hmmmm this should never happen
                matches = (ConvertID(rv) == ConvertID(pattern.Score));

            } else if (pattern.SearchType == SearchType.Text) {
                /*
                 *  -----c-----
                 *  So here we want to mimic a database and make this search case independent .... But Redis is not a fucking database - the text searches are binary implementations
                 *  Note that the string comparisons are conducted as a binary array of bytes (http://redis.io/commands/zrangebylex and https://en.wikipedia.org/wiki/UTF-8).  
                 *  This means that the first character has the most importance.  Note also that uppercase letters precede lower case letters
                 *  So here we want to do both upper and lower case searches (obvs for nums and chars this is not needed!)
                 *  Also, to mimic the * wildcard we need to search up until the next character (e.g. >= a and < b)... there are of course some wiggly special cases here 
                 *  To get all z's you need to search until { and to get all Z's you need to search until [, because these chars are ordered as UTF-8 (see http://www.fileformat.info/info/charset/UTF-8/list.htm )
                */

                // 24-Aug-2016 - Catch null entries ...
                if ( rv.IsNull == false) {
                    matches = rv.ToString().ToLower().StartsWith(pattern.PatternStartsWith(RedisWrapper.TextIndexLength));
                }
                

            } else if (pattern.SearchType == SearchType.Bool) {

                // 11-Aug-2016 - OK we have modded the bool values, so that they are stored as just zero or one
                matches = (pattern.Score == (int)rv);

//                if (pattern.Score == 1) {
//                    matches = (rv.ToString().Equals("true", StringComparison.CurrentCultureIgnoreCase));
//                } else {
//                    matches = (rv.ToString().Equals("false", StringComparison.CurrentCultureIgnoreCase));
//                }               

            } else if (pattern.SearchType == SearchType.DateTime) {

                long ticks = (long)rv;
                if (pattern.SearchComparisonExpression == SearchComparisonExpression.Equivalent) {
                    matches = (pattern.Score == ticks);
                } else if (pattern.SearchComparisonExpression == SearchComparisonExpression.RangeGreaterThanOrEqualTo) {
                    matches = (pattern.ScoreMin <= ticks);
                } else if (pattern.SearchComparisonExpression == SearchComparisonExpression.RangeLessThanOrEqualTo) {
                    matches = (pattern.ScoreMax >= ticks);
                } else if (pattern.SearchComparisonExpression == SearchComparisonExpression.RangeBetween) {
                    matches = (pattern.ScoreMin <= ticks && pattern.ScoreMax >= ticks);
                }

            } else if (pattern.SearchType == SearchType.Score) {

                double d = (double)rv;
                if (pattern.SearchComparisonExpression == SearchComparisonExpression.Equivalent) {
                    matches = (pattern.Score == d);
                } else if (pattern.SearchComparisonExpression == SearchComparisonExpression.RangeGreaterThanOrEqualTo) {
                    matches = (pattern.ScoreMin <= d);
                } else if (pattern.SearchComparisonExpression == SearchComparisonExpression.RangeLessThanOrEqualTo) {
                    matches = (pattern.ScoreMax >= d);
                } else if (pattern.SearchComparisonExpression == SearchComparisonExpression.RangeBetween) {
                    matches = (pattern.ScoreMin <= d && pattern.ScoreMax >= d);
                }

            }

            return matches;
        }




        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>
        ///     Warning o(N), so slooooow!!
        ///     Searches the values stored for a particular type of object.
        ///     override// override this do to a hash scan for when there are no indexes ...
        /// </summary>
        /// <param name="objectName">The name of the object to extract from the database</param>
        /// <param name="patterns">
        ///     A list of propertyNames (the keys) and the patterns to search for in each of these properties (the values)
        /// </param>
        /// <param name="doIntersect">
        ///     If true, the intersection between the patterns provided will be checked, otherwise the union will be returned.  
        ///     Not that this is equivalent to "and" and "or" respectively in a traditional RDBMS
        /// </param>
        //        public List<long> ScanObjects(string objectName, RedisSearchPattern pattern) {
        //            List<long> objs = new List<long>();
        //        public async Task<RedisValue[]> ScanObjects(string objectName, RedisSearchPattern pattern) {
        public async Task<List<uint>> ScanObjects(string objectName, DatabaseSearchPattern pattern) {
            List<uint> objIDs = new List<uint>();
            //List<RedisValue> objs = new List<RedisValue>();

            //-----c----- If this data item is indexed, lets use the indexes!  Otherwise we need to scan through all the objects...
            // No index so we need to scan through all the objects here ...
            //List<KeyValuePair<RedisValue, Task<RedisValue>>> tasks = new List<KeyValuePair<RedisValue, Task<RedisValue>>>();
            List<uint> keys = new List<uint>();
            List<Task<RedisValue>> tasks = new List<Task<RedisValue>>();

            //-----d-----
            foreach (RedisValue rv in rDBs[CurrentConnection].SetScan(KeyName(objectName), "*", 1000, CommandFlags.None)) {
                //objectName + IndexPrimaryKeySuffix, "*", 1000, CommandFlags.None)) {
                
                // Get the ID from the keys
                //                long id = 0;
                //                long.TryParse(rv.ToString(), out id);
                //                long id = (long)rv;

                keys.Add(ConvertID(rv));
            }

            //-----e-----
            foreach (uint rv in keys) {
                string hashName = KeyName(objectName, rv); // objectName + ":" + rv;

                // and then get the actual value we want to index here
                tasks.Add(rDBs[CurrentConnection].HashGetAsync(hashName, pattern.Parameter));

            }

            // get a list of key value pairs ...
            List<KeyValuePair<uint, RedisValue>> kvps = null;
            if (DoAsync == true) {
                kvps = await AwaitResults(keys, tasks);
            } else {
                kvps = AwaitResults(keys, tasks).Result;
            }


            // and now lets get all the values!!
            foreach (KeyValuePair<uint, RedisValue> kvp in kvps) {
                //                await t.Value;

                //long id = t.Key;
                string indexVal = kvp.Value; // t.Value.Result;

                if (pattern.SearchType == SearchType.Text) {
                    //if (indexVal.CompareTo(pattern.PatternMin) >= 0 && indexVal.CompareTo(pattern.PatternMax) <= 0) {
                    if (indexVal.CompareTo(pattern.PatternMin) >= 0 && indexVal.CompareTo(pattern.PatternMax) < 0) {
                        objIDs.Add(kvp.Key);
                    }
                } else if (pattern.SearchType == SearchType.Score || pattern.SearchType == SearchType.DateTime) {

                    double iv = 0;
                    double.TryParse(indexVal, out iv);

                    if (iv >= pattern.ScoreMin && iv <= pattern.ScoreMax) {
                        //objs.Add(t.Key);
                        objIDs.Add(kvp.Key);
                    }

                } else if (pattern.SearchType == SearchType.Bool) {

                    bool iv = false;
                    bool.TryParse(indexVal, out iv);

                    if ((iv == true && pattern.ScoreMin == 1) || (iv == false && pattern.ScoreMin == 0)) {
                        //objs.Add(t.Key);
                        objIDs.Add(kvp.Key);
                    }

                }
            }


            //return objs.ToArray();
            return objIDs;
        }





        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        public List<TestObj> ScanObjects(string objectName, List<TestObj> objs, List<DatabaseSearchPattern> patterns, bool doIntersect) {
            List<TestObj> rtos = new List<TestObj>();

            //-----c----- If this data item is indexed, lets use the indexes!  Otherwise we need to scan through all the objects...
            // No index so we need to scan through all the objects here ...
            foreach (TestObj rto in objs) {

                int patternCount = 0;
                bool isFound = false;
                foreach (DatabaseSearchPattern pattern in patterns) {
                    bool tempIsFound = false;

                    if (pattern.SearchType == SearchType.Text) {
                        //                        string oppMin, oppMax;
                        //                        bool doOpposite = pattern.GetOppositeCaseMaxAndMin(out oppMin, out oppMax);

                        // https://msdn.microsoft.com/en-us/library/az24scfc(v=vs.110).aspx
                        //if (Regex.IsMatch(rto.TestStr, "^[" + pattern.PatternMin + "].*$", RegexOptions.IgnoreCase)) {
                        //if (Regex.IsMatch(rto.TestStr, "^[" + pattern.PatternStartsWith + "].*$", RegexOptions.IgnoreCase)) {

                        StringBuilder txtPattern = new StringBuilder();
                        foreach( char c in pattern.PatternStartsWith(RedisWrapper.TextIndexLength).ToCharArray()) {
                            txtPattern.Append("[" + c + "]");
                        }
                        if (Regex.IsMatch(rto.TestStr, "^" + txtPattern + ".*$", RegexOptions.IgnoreCase)) {
                            //if (Regex.IsMatch(rto.TestStr, "^" + pattern.PatternStartsWith + ".*$", RegexOptions.IgnoreCase)) {
                            //                        if (rto.TestStr.CompareTo(pattern.PatternMin) >= 0 && rto.TestStr.CompareTo(pattern.PatternMax) <= 0) {
                            tempIsFound = true;
                            //                        } else if (doOpposite == true && Regex.IsMatch(rto.TestStr, "^" + oppMin + "[.]*$")) {
                            //                            tempIsFound = true;
                        }

                    } else if (pattern.SearchType == SearchType.PrimaryKey) {
                        if (rto.ID == ConvertID(pattern.Score)) {
                            tempIsFound = true;
                        }

                    } else if (pattern.SearchType == SearchType.Score) {

                        //if (pattern.Parameter.Equals("ID")) {
                        //    if (rto.ID == pattern.Score) {
                        //        tempIsFound = true;
                        //    }
                        //} else 
                        if (pattern.Parameter.Equals("TestInt")) {
                            if (rto.TestInt >= pattern.ScoreMin && rto.TestInt <= pattern.ScoreMax) {
                                tempIsFound = true;
                            }

                        } else if (pattern.Parameter.Equals("TestLong")) {
                            if (rto.TestLong >= pattern.ScoreMin && rto.TestLong <= pattern.ScoreMax) {
                                tempIsFound = true;
                            }

                        } else if (pattern.Parameter.Equals("TestDouble")) {
                            if (rto.TestDouble >= pattern.ScoreMin && rto.TestDouble <= pattern.ScoreMax) {
                                tempIsFound = true;
                            }
                            //                        } else {
                            //                            string ohFuck = "";
                        }


                    } else if (pattern.SearchType == SearchType.DateTime) {

                        if (rto.TestDT.Ticks >= (long)pattern.ScoreMin && rto.TestDT.Ticks <= (long)pattern.ScoreMax) {
                            tempIsFound = true;
                        }

                    } else if (pattern.SearchType == SearchType.Bool) {

                        if ((rto.TestBool == true && pattern.Score == 1) || (rto.TestBool == false && pattern.Score == 0)) {
                            tempIsFound = true;
                        }
                    }

                    // if we did not find it and this is an intersection, then break out!
                    if (tempIsFound == false) {
                        isFound = false;
                        break;
                    } else {
                        // tentatively record this as being found (pending all the remaining searches)...
                        isFound = true;
                    }

                    patternCount++;
                }

                //  Patterns completed, so if this object has been found, then lets add it!!
                if (isFound == true) {
                    rtos.Add(rto);
                }

            }

            return rtos;
        }



        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        public List<object> GetAllObjects(object obj) {
            List<object> objs = new List<object>();

            string objName = null;
            PropertyInfo[] objPI = null;
            GetObjectProperties(obj, out objName, out objPI);
//            Type type = obj.GetType();

            //-----a----- Get all of the objects!!!  This is potentially quite bad!!!
            //foreach (SortedSetEntry sse in rDB.SortedSetScan(objectName + IndexPrimaryKeySuffix, "*", (int)rDB.SortedSetLength(objectName + IndexPrimaryKeySuffix), CommandFlags.None)) {
            List<Task<HashEntry[]>> tasks = new List<Task<HashEntry[]>>();

            foreach (RedisValue rv in rDBs[CurrentConnection].SetScan(objName + IndexPrimaryKeySuffix, "*", 1000, CommandFlags.None)) {
                //objs.Add(GetObject((long)sse.Element));
                //objs.Add(GetObject((long) rv));
                //uint id = ConvertID(rv);
                tasks.Add(GetObjectStart(ConvertID(rv), objName));
            }

            // and iterate through the tasks to get our values
            foreach (Task<HashEntry[]> t in tasks) {
                objs.Add(GetObjectFinish(obj, t.Result, objPI));
            }


            return objs;
        }
        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        public List<TestObj> GetAllObjectsOfTypeTestObj() {
            List<TestObj> objs = new List<TestObj>();

            string objName = null;
            PropertyInfo[] objPI = null;
            GetObjectProperties(new TestObj(), out objName, out objPI);
            
            //-----a----- Get all of the objects!!!  This is potentially quite bad!!!
            //foreach (SortedSetEntry sse in rDB.SortedSetScan(objectName + IndexPrimaryKeySuffix, "*", (int)rDB.SortedSetLength(objectName + IndexPrimaryKeySuffix), CommandFlags.None)) {
            List<Task<HashEntry[]>> tasks = new List<Task<HashEntry[]>>();

            foreach (RedisValue rv in rDBs[CurrentConnection].SetScan(objName + IndexPrimaryKeySuffix, "*", 1000, CommandFlags.None)) {

                //objs.Add(GetObject((long)sse.Element));
                //objs.Add(GetObject((long) rv));
                //tasks.Add(GetObjectStart((ulong)rv));
                tasks.Add(GetObjectStart(ConvertID(rv)));
            }

            // and iterate through the tasks to get our values
            foreach (Task<HashEntry[]> t in tasks) {
                objs.Add(GetObjectFinish(t));
            }

            return objs;
        }



        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        //        public List<Task<HashEntry[]>> GetObjectsStart(object obj, List<RedisValue> objIDs) {
        public List<Task<HashEntry[]>> GetObjectsStart(object obj, List<uint> objIDs) {
            //List<object> objectList = new List<object>();

            //List<RedisValue> output = new List<RedisValue>();
            string oName = null;
            PropertyInfo[] oPI = null;
            GetObjectProperties(obj, out oName, out oPI);

            // Cycle through the tasks and skip over them (do not block) if they have not yet completed...
            //int numFaults = 0;
            List<Task<HashEntry[]>> tasks = new List<Task<HashEntry[]>>();

            //foreach (RedisValue rv in objIDs) {
            foreach (uint id in objIDs) {
                tasks.Add(GetObjectStart(id, oName));
            }

            return tasks;
        }
        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        //public List<Task<HashEntry[]>> GetObjectsStart(object obj, List<long> objIDs) {
        //    //List<object> objectList = new List<object>();

        //    //List<RedisValue> output = new List<RedisValue>();
        //    string oName = null;
        //    PropertyInfo[] oPI = null;
        //    GetObjectProperties(obj, out oName, out oPI);

        //    // Cycle through the tasks and skip over them (do not block) if they have not yet completed...
        //    //int numFaults = 0;
        //    List<Task<HashEntry[]>> tasks = new List<Task<HashEntry[]>>();

        //    foreach (RedisValue rv in objIDs) {
        //        tasks.Add(GetObjectStart(rv, oName));
        //    }

        //    return tasks;
        //}

        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        public List<object> GetObjectsFinish(object obj, List<Task<HashEntry[]>> tasks) {
            // async
            //List<Task<object>> objectList = new List<Task<object>>();
            List<object> objectList = new List<object>();

            //List<RedisValue> output = new List<RedisValue>();
            string oName = null;
            PropertyInfo[] oPI = null;
            GetObjectProperties(obj, out oName, out oPI);

            // Cycle through the tasks and skip over them (do not block) if they have not yet completed...
            //bool[] tasksCompleted = new bool[tasks.Count];

            bool allTasksCompleted = false;

            int numIterationsToFinalise = 0;
            int numFaults = 0;
            int numTasksCompleted = 0;

            // loop through all of the objects and check whether or not they have completed
            while (allTasksCompleted != true) {

                allTasksCompleted = true;

                // loop through and check each task to see if it has completed - if so, then lets suck up the results ...
                //int taskCounter = 0;
                for (int i = 0; i < tasks.Count; i++) {
                    Task<HashEntry[]> t = tasks[i];

                    if (t != null) {
                        if (t.IsCompleted == true) {
                            //if (tasksCompleted[taskCounter] == false) {
                            if (t.IsFaulted == true) {
                                numFaults++;
                                Logger.LogWarning("Task has faulted - " + t.Exception.ToString() + " with inner exception: " + t.Exception.InnerException.ToString());
                            } else {
                                //-----Option 1 - Just add the range
                                //await new Task(new Action<object, HashEntry[], PropertyInfo[]>( GetObjectFinish));
                                //objectList.Add( await GetObjectFinish(obj, t.Result, oPI));
                                //object o = await Task.Run(() => GetObjectFinish(obj, t.Result, oPI));
                                objectList.Add(GetObjectFinish(obj, t.Result, oPI));
                                //objectList.Add(o);

                            }

                            numTasksCompleted++;

                            // set the task to have completed ...
                            //tasksCompleted[taskCounter] = true;
                            //}
                        } else {
                            //if (DoAsync == true) {
                            //                                    await t;
                            //                                }

                            allTasksCompleted = false;
                            // it is still going - so what is the best approach here ???
                        }
                    }
                    //taskCounter++;
                }

                // lets do a quick check of how many tasks have been completed so far ...

                //foreach (bool b in tasksCompleted) {
                //                    numTasksCompleted = numTasksCompleted + ((b == true) ? 1 : 0);
                //                }
                Logger.Log(numTasksCompleted, 1);
                if (allTasksCompleted == false) {
                    Thread.Sleep(1); // Have a cup of tea!!!
                }
                numIterationsToFinalise++;
            }



            return objectList;
        }



        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>
        ///         Get an object from the database given an ID
        /// </summary>
        /// <param name="objectName"></param>
        /// <param name="id"></param>
        /// <returns></returns>
        public bool GetObject(uint id, object obj) {
            bool success = true; // innocent until proven guilty ...

            // now lets interrogate this object to see what we have got ...
            if (obj == null) {
                success = false;
                Logger.LogWarning("GetObject - the object provided was null!  So we can't use reflection to determine what it was ...  You should provide an empty object ready for populating!");
            } else {
                // lets get the property information for this object ...
                //Type type = obj.GetType();
                string objName = null;
                PropertyInfo[] objPI = null;
                GetObjectProperties(obj, out objName, out objPI);


                Task<HashEntry[]> task = GetObjectStart(id, objName);
                if (task != null) {
                    task.Wait();
                    obj = GetObjectFinish(obj, task.Result, objPI);
                }


            }


            return success;
        }
        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>
        ///         Get an object from the database given an ID
        /// </summary>
        /// <param name="objectName"></param>
        /// <param name="id"></param>
        /// <returns></returns>
        public Task<HashEntry[]> GetObjectStart(uint id, string objectName) {
            //bool success = true; // innocent until proven guilty ...
            //Task<bool> = null;
            //Task<HashEntry[]> t = null;

            try {
                // and then get the data from the db here
                //string hashName = KeyName(objectName, id); // objectName + ":" + id;
                return rDBs[CurrentConnection].HashGetAllAsync(KeyName(objectName, id));
            } catch (Exception ex) {
                string ohbugger = "";
            }

            return null;
        }

        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        public object GetObjectFinish(object obj, HashEntry[] data, PropertyInfo[] typeProperties) {
            // async
            //bool success = true; // innocent until proven guilty ...
            object newObj = null;

            //Task<object> task = null;

            try {
                //                Func<RedisValue[]> pk = delegate () { return new RedisValue[] { pattern.Score }; };
                //                task = new Task<RedisValue[]>(pk);

                /////////////////////////
                // Hmmm - this doesnt work here ....

                // Need to look at this ........................................................
                ///////////////////////////////////////////////////////////////////////////////////////

                //                Func<object, object> t = delegate (object o) {
                newObj = Activator.CreateInstance(obj.GetType());

                // Now go through the data we have and try to match it against the properties we have - obviously will need the relevant parsing ...
                foreach (HashEntry he in data) {
                    foreach (PropertyInfo pi in typeProperties) {
                        if (he.Name.Equals(pi.Name) == true) {

                            // Only worth continuing if our object is writeable...
                            if (pi.CanWrite == true) {

                                object v = null;

                                // DateTime, Boolean, Int32, Int64, Double, String .... might need to do some fancy switching here ..
                                if (pi.PropertyType.Name == "Int64") {
                                    long temp = 0;
                                    long.TryParse(he.Value, out temp);
                                    v = temp;
                                } else if (pi.PropertyType.Name == "UInt64") {
                                    ulong temp = 0;
                                    ulong.TryParse(he.Value, out temp);
                                    v = temp;
                                } else if (pi.PropertyType.Name == "UInt32") {
                                    uint temp = 0;
                                    uint.TryParse(he.Value, out temp);
                                    v = temp;
                                } else if (pi.PropertyType.Name == "Int32") {
                                    int temp = 0;
                                    int.TryParse(he.Value, out temp);
                                    v = temp;
                                } else if (pi.PropertyType.Name == "Double") {
                                    double temp = 0;
                                    double.TryParse(he.Value, out temp);
                                    v = temp;
                                } else if (pi.PropertyType.Name == "String") {
                                    v = he.Value.ToString();
                                } else if (pi.PropertyType.Name == "DateTime") {
                                    long temp = 0;
                                    long.TryParse(he.Value, out temp);
                                    DateTime temp2 = new DateTime(temp);
                                    v = temp2;
                                } else if (pi.PropertyType.Name == "Boolean") {
                                    bool temp = ((int)he.Value == 1) ? true : false;
                                    //bool.TryParse(he.Value, out temp);
                                    v = temp;
                                }

                                pi.SetValue(newObj, v);
                            }

                            break;
                        }
                    }
                }

                //                };

                //                Task<object>.Run(() => return 123;);

                //                task = t; // new Task<object>(t(obj));
                //await task;

            } catch (Exception ex) {
                //                success = false;
                //newObj = null;
                Logger.LogWarning("GetObject - populating the object crashed - with the specific exception: " + ex.ToString());

            }

            return newObj;
            //return task;
        }



        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>
        ///         Get an object from the database given an ID
        /// </summary>
        /// <param name="objectName"></param>
        /// <param name="id"></param>
        /// <returns></returns>
        public bool GetObjectProperties(object obj, out string objectName, out PropertyInfo[] objectPI) {
            bool success = true; // innocent until proven guilty ...

            objectName = null;
            objectPI = null;

            // now lets interrogate this object to see what we have got ...
            if (obj == null) {
                success = false;
                Logger.LogWarning("GetObjectProperties - the object provided was null!  So we can't use reflection to determine what it was ...  You should provide an empty object ready for populating!");
            } else {
                // lets get the property information for this object ...
                Type type = obj.GetType();
                objectName = type.Name;
                objectPI = type.GetProperties();

                // 15-Aug-2016 - if the object has a property called "ObjectName", then lets override the default type name (this may e.g. be a RedisPerformance improvement)
                foreach( PropertyInfo pi in objectPI) {
                    if ( pi.Name.Equals( "ObjectName", StringComparison.CurrentCultureIgnoreCase)) {
                        objectName = pi.GetValue(obj).ToString();
                        break;
                    }
                }
            }

            return success;
        }

        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        // Testing the non reflection approach ....
        // The RV should be a long
        public Task<HashEntry[]> GetObjectStart(uint id) {
            //bool success = true; // innocent until proven guilty ...
            Task<HashEntry[]> task = null;

            try {

                // and then get the data from the db here
                string hashName = KeyName(TestObj.ObjectName, id); // + ":" + id;
                task = rDBs[CurrentConnection].HashGetAllAsync(hashName);

            } catch (Exception ex) {
                //success = false;
                Logger.LogWarning("GetObjectStart - extracting the object data crashed - with the specific exception: " + ex.ToString());

            }

            //return success;
            return task;
        }

        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        public TestObj GetObjectFinish(Task<HashEntry[]> task) {
            //bool success = true; // innocent until proven guilty ...
            TestObj rto = new TestObj();

            try {

                if (task.IsCompleted == false) {
                    task.Wait();
                }

                // Now go through the data we have and try to match it against the properties we have - obviously will need the relevant parsing ...
                foreach (HashEntry he in task.Result) {

                    if (he.Name.Equals("ID") == true) {
                        ulong temp = 0;
                        ulong.TryParse(he.Value, out temp);
                        rto.ID = temp;

                    } else if (he.Name.Equals("TestInt") == true) {
                        int temp = 0;
                        int.TryParse(he.Value, out temp);
                        rto.TestInt = temp;

                    } else if (he.Name.Equals("TestLong") == true) {
                        long temp = 0;
                        long.TryParse(he.Value, out temp);
                        rto.TestLong = temp;

                    } else if (he.Name.Equals("TestDouble") == true) {
                        double temp = 0;
                        double.TryParse(he.Value, out temp);
                        rto.TestDouble = temp;

                    } else if (he.Name.Equals("TestBool") == true) {
                        //bool temp = false;
                        //bool.TryParse(he.Value, out temp);
                        bool temp = ((int)he.Value == 1) ? true : false;
                        rto.TestBool = temp;

                    } else if (he.Name.Equals("TestStr") == true) {
                        rto.TestStr = he.Value;

                    } else if (he.Name.Equals("TestDT") == true) {
                        long temp = 0;
                        long.TryParse(he.Value, out temp);
                        rto.TestDT = new DateTime(temp);
                    }
                }

            } catch (Exception ex) {
                //success = false;
                Logger.LogWarning("GetObject - populating the object crashed - with the specific exception: " + ex.ToString());

            }

            //return success;
            return rto;
        }


        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        // Get all the results as they become available without blocking (or at least minimising the blocking )...
        //public static List<RedisValue> AwaitResults(Task<List<RedisValue>> task) {
        //public static List<RedisValue> AwaitResults(Task<List<long>> task) {
        public List<uint> AwaitResults(Task<List<uint>> task) {
            //List<RedisValue> output = new List<RedisValue>();

            // Cycle through the tasks and skip over them (do not block) if they have not yet completed...
            bool taskCompleted = false;

            int numIterationsToFinalise = 0;

            int numFaults = 0;

            // loop through all of the objects and check whether or not they have completed
            while (taskCompleted != true) {

                if (task.IsCompleted == true) {
                    if (task.IsFaulted == true) {
                        numFaults++;
                    } else {
                        //-----Option 1 - Just add the range
                        // just lets go!!!
                        return task.Result;
                    }
                    // set the task to have completed ...
                    taskCompleted = true;
                } else {
                    numIterationsToFinalise++;
                }
            }

            // If we are here it has all gone to shit
            // Warn about any faults here .......
            Logger.Log("Waited for the results and had to run " + numIterationsToFinalise + " iterations to make it so ");
            if (numFaults > 0) {
                Logger.LogWarning("RedisWrapper.AwaitResults - Faults observed while waiting for results = " + numFaults.ToString("N0") + ".");
            }
            return null;
        }



        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        // Get all the results as they become available without blocking (or at least minimising the blocking )...
        //        public static List<RedisValue> AwaitResults(List<Task<RedisValue[]>> tasks) {
        //        public async static Task<HashSet<long>> AwaitResults(List<Task<RedisValue[]>> tasks) {
        /// <summary>
        ///     Note that making this method async and using e.g WhenAll makes this about 5% slower
        /// </summary>
        /// <param name="tasks"></param>
        /// <returns></returns>
        public List<uint> AwaitResults(List<Task<RedisValue[]>> tasks) {
            //List<RedisValue> output = new List<RedisValue>();
            //List<uint> output = new List<uint>();
            HashSet<uint> output = new HashSet<uint>();
            //HashSet<RedisValue> output = new HashSet<RedisValue>();

            // Cycle through the tasks and skip over them (do not block) if they have not yet completed...
            bool[] tasksCompleted = new bool[tasks.Count];

            bool allTasksCompleted = false;

            int numIterationsToFinalise = 0;

            int numFaults = 0;

            //            while (Task.WaitAll(tasks.ToArray(), 1000) == false) {
            //                Thread.Sleep(10);
            //            }

            /*
            RedisValue[][] allSets = await Task.WhenAll(tasks);

            foreach (RedisValue[] rvs in allSets) {
                foreach (RedisValue rv in rvs) {
                    long lo = (long)rv;
                    if (output.Contains(lo) == false) {
                        output.Add(lo);
                    }
                }
            }
            */


            // loop through all of the objects and check whether or not they have completed

            while (allTasksCompleted != true) {

                allTasksCompleted = true;

                // loop through and check each task to see if it has completed - if so, then lets suck up the results ...
                int taskCounter = 0;
                foreach (Task<RedisValue[]> t in tasks) {

                    //await t;
                    //                for (int j = 0; j < rvTasks.Count; j++) {
                    //                    Task<RedisValue[]> t = rvTasks[j];
                    //long memUsage = GC.GetTotalMemory(false);

                    if (t.IsCompleted == true) {
                        if (tasksCompleted[taskCounter] == false) {
                            if (t.IsFaulted == true) {
                                string ohFuck = "";

                                // Cluster is down exception - probably only momentarily ...
                                // Hmmm no - if one is faulted, all the remainder will be too.
                                //Thread.Sleep(100);
                                //allTasksCompleted = false;
                                numFaults++;
                            } else {
                                //-----Option 1 - Just add the range
                                //output.AddRange(t.Result);

                                //-----Option 2 - cast each value - about equivalent speed
                                foreach (RedisValue rv in t.Result) {
                                    output.Add(ConvertID(rv));
                                }
                                //-----Option 3 - convert each RedisValue to a string and then parse as a long - about 20% slower
                                //foreach (RedisValue rv in t.Result) {
                                //    long lo = 0;
                                //    long.TryParse(rv.ToString(), out lo);
                                //    rvHPs2.Add(lo);
                                //}

                            }
                            // set the task to have completed ...
                            tasksCompleted[taskCounter] = true;
                            // remove the completed task so we dont add the info twice!!!
                            //                            rvTasks.RemoveAt(j);
                            //                            j--;
                        }
                    } else {
                        allTasksCompleted = false;
                        // it is still going - so what is the best approach here ???
                    }
                    taskCounter++;
                }

                // lets do a quick check of how many tasks have been completed so far ...
                if (allTasksCompleted == false) {
                    int numTasksCompleted = 0;
                    foreach (bool b in tasksCompleted) {
                        numTasksCompleted = numTasksCompleted + ((b == true) ? 1 : 0);
                    }
                    Logger.Log(numTasksCompleted, 1);

                    Thread.Sleep(1); // Have a cup of tea!!! (10)
                }
                numIterationsToFinalise++;
            }


            // Warn about any faults here .......
            Logger.Log("Waited for the results and had to run " + numIterationsToFinalise + " iterations to make it so ");
            Logger.Log("Total num objects = " + output.Count.ToString("N0") + ".  And number of faults observed = " + numFaults.ToString("N0") + ".");
            if (numFaults > 0) {
                Logger.LogWarning("RedisWrapper.AwaitResults - Faults observed while waiting for results = " + numFaults.ToString("N0") + ".");
            }

            // job done ...
            return output.ToList();
        }

        /*
        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        // Get all the results as they become available without blocking (or at least minimising the blocking )...
        /// <summary>
        ///     Making this async is slooooower!!
        /// </summary>
        /// <param name="tasks"></param>
        /// <returns></returns>
        //public async static Task<HashSet<long>> AwaitResults(List<Task<HashSet<long>>> tasks) {
        public List<List<uint>> AwaitResultsSeparately(List<Task<List<uint>>> tasks) {
            //public async static Task<HashSet<long>> AwaitResults(List<Task<HashSet<long>>> tasks) {
            //List<long> output = new List<long>();
            //HashSet<RedisValue> output = new HashSet<RedisValue>();
            //List<uint> output = new List<uint>();
            //HashSet<uint> output = new HashSet<uint>();
            List<List<uint>> output = new List<List<uint>>(tasks.Count);
            for(int i = 0; i < tasks.Count; i++) {
                output.Add(new List<uint>());
            }

            int startNum = tasks.Count;
            Logger.Log("\nAwaiting results separately for " + startNum + " tasks");

            bool allTasksCompleted = false;

            int numIterationsToFinalise = 0;

            int numFaults = 0;


            // loop through all of the objects and check whether or not they have completed
            while (allTasksCompleted != true) {

                allTasksCompleted = true;

                // loop through and check each task to see if it has completed - if so, then lets suck up the results ...
                //int taskCounter = 0;
                for (int i = 0; i < tasks.Count; i++) {
                    Task<List<uint>> t = tasks[i];
                    if (t != null) {
                        if (t.IsCompleted == true) {
                            int resultCount = 0;

                            if (t.IsFaulted == true) {
                                numFaults++;

                            } else {
                                //-----Option 1 - Just add the range
                                output[i] = t.Result;
                            }

                            DebugFinish(t.Id, t.IsFaulted, resultCount);

                            // set the task to have completed ...
                            // remove the completed task so we dont add the info twice!!!
                            tasks[i] = null;
                        } else {

                            allTasksCompleted = false;
                            // it is still going - so what is the best approach here ???
                        }
                    }
                    //taskCounter++;
                }

                int numTasksCompleted = startNum - tasks.Count;
                Logger.Log(numTasksCompleted, 1);
                if (allTasksCompleted == false) {
                    Thread.Sleep(1); // Have a cup of tea!!! (500)
                }
                numIterationsToFinalise++;
            }

            
            // Warn about any faults here .......
            //Logger.Log("Waited for the results and had to run " + numIterationsToFinalise + " iterations to make it so ");
            Logger.Log("Number of faults observed = " + numFaults.ToString("N0")
                + ".  Had to run " + numIterationsToFinalise + " iterations to make it so");
            
            //Logger.Log("Total num objects = " + output.Count.ToString("N0") + ".  And number of faults observed = " + numFaults.ToString("N0")+ ".");
            if (numFaults > 0) {
                Logger.LogWarning("RedisWrapper.AwaitResults - Faults observed while waiting for results = " + numFaults.ToString("N0") + ".");
            }

            // job done ...
            return output;
        }
        */

        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        public bool AwaitTasks(List<Task<bool>> tasks, string genericErrorMessage) {
            bool success = false;

            //-----e----- Check it all ran successfully in a soft manner!
            if (tasks.Count > 0) {
                success = true;
                int numFaults = 0;
                int numCompleted = 0;
                bool allTasksCompleted = false;
                while (allTasksCompleted == false) {

                    allTasksCompleted = true;

                    for (int i = 0; i < tasks.Count; i++) {
                        Task<bool> t = tasks[i];

                        if (t != null) {
                            if (t.IsCompleted == true) {
                                if (t.IsFaulted == true) {
                                    numFaults++;
                                    Logger.LogWarning("Task " + t.Id + " faulted while "+genericErrorMessage + ".  The specific exception was: "+t.Exception.ToString());
                                } else {
                                    success = success & t.Result;
                                }
                                // remove the completed task so we dont add the info twice!!!
                                // we nullify completed results once they have completed, rather than waiting for the first result that has not completed
                                tasks[i] = null;
                                numCompleted++;
                            } else {
                                allTasksCompleted = false;
                            }
                        }
                    }

                    if (allTasksCompleted == false) {
                        Thread.Sleep(1);
                    }
                }
            }

            return success;
        }
        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        public bool AwaitTasks(List<Task> tasks, string genericErrorMessage) {
            bool success = false;

            //-----e----- Check it all ran successfully in a soft manner!
            if (tasks.Count > 0) {
                success = true;
                int numFaults = 0;
                int numCompleted = 0;
                bool allTasksCompleted = false;
                while (allTasksCompleted == false) {

                    allTasksCompleted = true;

                    for (int i = 0; i < tasks.Count; i++) {
                        Task t = tasks[i];

                        if (t != null) {
                            if (t.IsCompleted == true) {
                                if (t.IsFaulted == true) {
                                    numFaults++;
                                    Logger.LogWarning("Task " + t.Id + " faulted while " + genericErrorMessage + ".  The specific exception was: " + t.Exception.Message);
                                } else {
                                    success = success & true;
                                }
                                // remove the completed task so we dont add the info twice!!!
                                // we nullify completed results once they have completed, rather than waiting for the first result that has not completed
                                tasks[i] = null;
                                numCompleted++;
                            } else {
                                allTasksCompleted = false;
                            }
                        }
                    }

                    if (allTasksCompleted == false) {
                        Thread.Sleep(1);
                    }
                }
            }

            return success;
        }

        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        // Get all the results as they become available without blocking (or at least minimising the blocking )...
        /// <summary>
        ///     Making this async is slooooower!!
        /// </summary>
        /// <param name="tasks"></param>
        public List<uint> AwaitResults(List<Task<List<uint>>> tasks) {
            //
            HashSet<uint> output = new HashSet<uint>();
            //
            int startNum = tasks.Count;
            Logger.Log("\nAwaiting results for " + startNum + " tasks");
            //
            bool allTasksCompleted = false;
            //
            int numIterationsToFinalise = 0;
            //
            int numFaults = 0;
            int numTasksCompleted = 0;

            // Loop through all of the objects and check whether or not they have completed
            while (allTasksCompleted != true) {

                allTasksCompleted = true;

                // Loop through and check each task to see if it has completed - if so, then lets suck up the results ...
                //int taskCounter = 0;
                // Although slower than a foreach, this means we can cull the tasks from memory as soon as we are finished with them
                for (int i = 0; i < tasks.Count; i++) {
                    Task<List<uint>> t = tasks[i];

                    // 25-Aug-2016 - New approach - lets just nullify the tasks that have completed ...
                    if (t != null) {
                        if (t.IsCompleted == true) {
                            int resultCount = 0;

                            if (t.IsFaulted == true) {
                                numFaults++;
                                Logger.LogWarning("Task has faulted - " + t.Exception.ToString() + " with inner exception: " + t.Exception.InnerException.ToString());
                            } else {
                                //-----Option 1 - Just add the range
                                output.UnionWith(t.Result);
                                resultCount = t.Result.Count;
                            }

                            DebugFinish(t.Id, t.IsFaulted, resultCount);

                            // set the task to have completed ...
                            // remove the completed task so we dont add the info twice!!!
                            //tasks.RemoveAt(i);
                            //i--;
                            tasks[i] = null;
                            numTasksCompleted++;

                        } else {
                            if (DoAsync == true) {
                                ///////////////////////
                                //                            await t;
                            }

                            allTasksCompleted = false;
                            // it is still going - so what is the best approach here ???
                        }
                    }
                    //taskCounter++;
                }

                // lets do a quick check of how many tasks have been completed so far ...
                //                int numTasksCompleted = 0;
                //                foreach (bool b in tasksCompleted) {
                //                    numTasksCompleted = numTasksCompleted + ((b == true) ? 1 : 0);
                //                }
                //int numTasksCompleted = startNum - tasks.Count;
                Logger.Log(numTasksCompleted, 1);
                if (allTasksCompleted == false) {
                    Thread.Sleep(1); // Have a cup of tea!!! (500)
                }
                numIterationsToFinalise++;
            }




            // Warn about any faults here .......
            //Logger.Log("Waited for the results and had to run " + numIterationsToFinalise + " iterations to make it so ");
            Logger.Log("Total num objects = " + output.Count.ToString("N0") + ".  And number of faults observed = " + numFaults.ToString("N0")
                + ".  Had to run " + numIterationsToFinalise + " iterations to make it so");


            //Logger.Log("Total num objects = " + output.Count.ToString("N0") + ".  And number of faults observed = " + numFaults.ToString("N0")+ ".");
            if (numFaults > 0) {
                Logger.LogWarning("RedisWrapper.AwaitResults - Faults observed while waiting for results = " + numFaults.ToString("N0") + ".");
            }

            // job done ...
            //return output;
            return output.ToList();
        }



        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        // Get all the results as they become available without blocking (or at least minimising the blocking )...
        // Combines the keys with the values being extracted in the list of tasks.
        // THe list of keys should be the same length as the list of values ...
        public static async Task<List<KeyValuePair<uint, RedisValue>>> AwaitResults(List<uint> keys, List<Task<RedisValue>> tasks) {
            List<KeyValuePair<uint, RedisValue>> output = new List<KeyValuePair<uint, RedisValue>>();

            //if (keys.Count != tasks.Count) {
//                string ohFuck = "";
            //}

            // Cycle through the tasks and skip over them (do not block) if they have not yet completed...
            //bool[] tasksCompleted = new bool[keys.Count];
            //
            bool allTasksCompleted = false;
            //
            int numIterationsToFinalise = 0;
            //
            int numFaults = 0;
            int numTasksCompleted = 0;

            // loop through all of the objects and check whether or not they have completed
            while (allTasksCompleted != true) {

                allTasksCompleted = true;

                // loop through and check each task to see if it has completed - if so, then lets suck up the results ...
                //int taskCounter = 0;               

                for (int i = 0; i < tasks.Count; i++) {
                    Task<RedisValue> t = tasks[i];

                    if (t != null) {
                        if (t.IsCompleted == true) {
                            //                        if (tasksCompleted[taskCounter] == false) {
                            if (t.IsFaulted == true) {
                                numFaults++;
                                Logger.LogWarning("Task has faulted - " + t.Exception.ToString() + " with inner exception: " + t.Exception.InnerException.ToString());                               
                            } else {
                                //----- Just add the KVP
                                output.Add(new KeyValuePair<uint, RedisValue>(keys[i], t.Result));
                            }
                            // set the task to have completed ...
                            //                            tasksCompleted[taskCounter] = true;
                            // remove the completed task so we dont add the info twice!!!
                            //                            rvTasks.RemoveAt(j);
                            //                            j--;
                            tasks[i] = null;
                            numTasksCompleted++;
                            //}
                        } else {

                            //if (DoAsync == true) {
                                //await t;
                            //}

                            allTasksCompleted = false;
                            // it is still going - so what is the best approach here ???
                        }
//                    } else {
                        
                    }
                    //taskCounter++;
                }

                // lets do a quick check of how many tasks have been completed so far ...

                //foreach (bool b in tasksCompleted) {
                //                    numTasksCompleted = numTasksCompleted + ((b == true) ? 1 : 0);
                //                }
                Logger.Log(numTasksCompleted, 1);
                if (allTasksCompleted == false) {
                    Thread.Sleep(1); // Have a cup of tea!!! (100)
                }
                numIterationsToFinalise++;
            }


            // Warn about any faults here .......
            Logger.Log("Waited for the results and had to run " + numIterationsToFinalise + " iterations to make it so ");
            Logger.Log("Total num objects = " + output.Count.ToString("N0") + ".  And number of faults observed = " + numFaults.ToString("N0") + ".");
            if (numFaults > 0) {
                Logger.LogWarning("RedisWrapper.AwaitResults - Faults observed while waiting for results = " + numFaults.ToString("N0") + ".");
            }

            // job done ...
            return output;
        }



    }
}
