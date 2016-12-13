using MGL.Data.DataUtilities;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

//---------------------------------------------------------------------------------------------------------------------------------------------------------------------
namespace DataNirvana.Database {
    //------------------------------------------------------------------------------------------------------------------------------------------------------------------
    public class TestObj {

        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>
        ///     Abbreviated name to keep e.g. Redis memory usage to a minimum...
        /// </summary>
        public static string ObjectName {
            get { return objectName;  }
        }
        public static string objectName = "RT";  // Originally this test class was called RedisTestObj...

        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        public TestObj() {

        }
        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        public TestObj(ulong id, int testInt, long testLong, double testDouble, bool testBool, string testStr, DateTime testDT) {
            this.id = id;
            this.testInt = testInt;
            this.testLong = testLong;
            this.testDouble = testDouble;
            this.testBool = testBool;
            this.testStr = testStr;
            this.testDT = testDT;
        }

        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        public ulong ID {
            get { return id; }
            set { id = value; }
        }
        private ulong id = 0;


        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        public int TestInt {
            get { return testInt; }
            set { testInt = value; }
        }
        private int testInt = 0;

        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        public long TestLong {
            get { return testLong; }
            set { testLong = value; }
        }
        private long testLong = 0;

        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        public double TestDouble {
            get { return testDouble; }
            set { testDouble = value; }
        }
        private double testDouble = 0.0;

        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        public bool TestBool {
            get { return testBool; }
            set { testBool = value; }
        }
        private bool testBool = false;

        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        public string TestStr {
            get { return testStr; }
            set { testStr = value; }
        }
        private string testStr = "";

        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        public DateTime TestDT {
            get { return testDT; }
            set { testDT = value; }
        }
        private DateTime testDT = new DateTime();


        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        public static List<TestObj> ParseList(List<object> objs) {
            List<TestObj> rtos = null;
            if ( objs != null) {
                rtos = new List<TestObj>();

                foreach( object obj in objs) {
                    rtos.Add((TestObj)obj);
                }
            }
            return rtos;
        }

        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>
        ///     Avoid using dictionaries and sorted lists unless absolutely necessary!! Identifying values within these lists does not scale!!
        ///     Primarily because of the .Contains function....
        /// </summary>
        /// <param name="propertyName"></param>
        /// <param name="pi"></param>
        /// <param name="objs"></param>
        /// <returns></returns>
        public static List<KeyValuePair<uint, string>> BuildStringList(string propertyName, PropertyInfo[] pi, List<object> objs) {
            List<KeyValuePair<uint, string>> list = new List<KeyValuePair<uint, string>>();

            if (objs != null) {

                int counter = 0;
                foreach (object obj in objs) {
                    TestObj rto = (TestObj)obj;

                    foreach (PropertyInfo propInfo in pi) {
                        string propName = propInfo.Name;
                        if (propInfo.Name.Equals(propertyName, StringComparison.CurrentCultureIgnoreCase) == true) {
                            object sourceVal = propInfo.GetValue(rto, null);
                            if (sourceVal != null) {
                                // Convert the DateTime to ticks ...
                                if (propInfo.PropertyType.Name.Equals("DateTime", StringComparison.CurrentCultureIgnoreCase) == true) {
                                    sourceVal = ((DateTime)sourceVal).Ticks;
                                }
                                //string sourceValStr = sourceVal.ToString();
                                //if (useUInt == true) {
                                    list.Add(new KeyValuePair<uint, string>((uint)rto.ID, sourceVal.ToString()));
//                                } else {
   //                                 list.Add(new KeyValuePair<uint, string>((ulong)rto.ID, sourceVal.ToString()));
      //                          }
                                

                                //if (dict.Keys.Contains(sourceValStr) == false) {
                                //    dict.Add(sourceValStr, new List<long>() { rto.ID });
                                //} else {
                                //    List<long> valList = null;
                                //    dict.TryGetValue(sourceValStr, out valList);
                                //    if (valList.Contains(rto.ID) == false) {
                                //        valList.Add(rto.ID);
                                //    }
                                //    dict[sourceValStr] = valList;
                                //}
                            }
                        }
                    }
                    Logger.Log(++counter, 1000, objs.Count);
                }
                Logger.Log("");
            }
            return list;
        }

        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        public static List<KeyValuePair<uint, double>> BuildNumericList(string propertyName, PropertyInfo[] pi, List<object> objs) { //,bool useUInt) {
            List<KeyValuePair<uint, double>> list = new List < KeyValuePair <uint, double>>();

            if (objs != null) {
                int counter = 0;
                foreach (object obj in objs) {
                    TestObj rto = (TestObj)obj;

                    foreach (PropertyInfo propInfo in pi) {
                        string propName = propInfo.Name;
                        if (propInfo.Name.Equals(propertyName, StringComparison.CurrentCultureIgnoreCase) == true) {
                            object sourceVal = propInfo.GetValue(rto, null);
                            if (sourceVal != null) {
                                // Convert the DateTime to ticks ...
                                if (propInfo.PropertyType.Name.Equals("DateTime", StringComparison.CurrentCultureIgnoreCase) == true) {
                                    sourceVal = ((DateTime)sourceVal).Ticks;
                                } else if (propInfo.PropertyType.Name.Equals("Boolean", StringComparison.CurrentCultureIgnoreCase) == true) {
                                    sourceVal = ((bool)sourceVal == true) ? 1 : 0;
                                }

                                double sourceValD = 0;
                                double.TryParse( sourceVal.ToString(), out sourceValD);

                                //if (useUInt == true) {
                                    list.Add(new KeyValuePair<uint, double>((uint)rto.ID, sourceValD));
                                //} else {
                                    //list.Add(new KeyValuePair<RedisValue, double>((ulong)rto.ID, sourceValD));
                                //}

                                //list.Add(new KeyValuePair<RedisValue, double>(rto.ID, sourceValD));

                                //if (sList.Keys.Contains(sourceValD) == false) {
                                //    sList.Add(sourceValD, new List<long>() { rto.ID });
                                //} else {
                                //    List<long> valList = null;
                                //    sList.TryGetValue(sourceValD, out valList);
                                //    if (valList.Contains(rto.ID) == false) {
                                //        valList.Add(rto.ID);
                                //    }
                                //    sList[sourceValD] = valList;
                                //}
                            }
                        }
                    }
                    Logger.Log(++counter, 1000, objs.Count);
                }
                Logger.Log("");
            }
            return list;
        }


    }
}
