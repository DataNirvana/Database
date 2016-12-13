using System;
using System.Collections;
using System.Text;
using System.Collections.Generic;
using MGL.Data.DataUtilities;

// this should be in DataNirvana.Security...

//-----------------------------------------------------------------------------------------------------------------------------------------------------------------
namespace DataNirvana.Database {

    //-------------------------------------------------------------------------------------------------------------------------------------------------------------
    /**
        Description:	LoggerDB
        Type:				Logger
        Author:			Edgar Scrase
        Date:				October 2013
        Version:			2.0

        Notes:			Simple DB logging method
     *                      14-Jan-2015 - extended to also include the IP4 address of the request to enable more detailed logging of where the requests are coming from

    */
    public static class LoggerDB {

        private static string logTN = "Log_PageRequests";

        //-----------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>
        ///     14-Oct-13 - New method to log the page request information in the database ...
        /// </summary>
        public static void LogPageRequestInDatabase(ConfigurationInfo ci, string applicationName, string aspSessionID, string uniqueSessionID,
            string pageURL, DateTime pageRequestTime, double serverProcessingMS, int currentUserID, string ipAddress) {
            bool success = false;

            DatabaseWrapper dbInfo = null;

            try {

                //_____ Check for a null configurationInfo or dbConInfo object
                if (ci == null || ci.DbConInfo.NAME == null) {
                    Logger.LogError(200, "Error inserting the PageRequest log into the database - null configurationInfo found, probably because of an application reset or session timeout.");
                } else {

                    // 3-Mar-2016 - ignore specific IP addresses (e.g. robots like the UptimeRobot), as these are artificially bloating the logs with limited utility!
                    if (ipAddress == null || AddressesToIgnore.Contains(ipAddress) == false) {

                        //_____ Check for shite data and make it blank if so, so it doesn't kill the database ...
                        applicationName = (applicationName == null) ? "" : applicationName;
                        aspSessionID = (aspSessionID == null) ? "" : aspSessionID;
                        uniqueSessionID = (uniqueSessionID == null) ? "" : uniqueSessionID;
                        pageURL = (pageURL == null) ? "" : pageURL;

                        //_____ Connect to the database ...
                        dbInfo = new DatabaseWrapper(ci);
                        dbInfo.Connect();

                        //_____ Create the table if it does not already exist
                        string createTableSQL = @"
                    CREATE TABLE " + logTN + @" (
                        ID int NOT NULL Auto_Increment, PRIMARY KEY(ID),
                        Application_Name VARCHAR(255), INDEX Application_Name(Application_Name),
                        Session_ID_ASP VARCHAR(50), INDEX Session_ID_ASP(Session_ID_ASP),
                        Session_ID_Unique VARCHAR(50), INDEX Session_ID_Unique(Session_ID_Unique),
                        Page_Name VARCHAR(255), INDEX Page_Name(Page_Name),
                        Page_URL LONGTEXT,
                        Page_Request_Date DATETIME, INDEX Page_Request_Date(Page_Request_Date),
                        Server_Render_Speed DOUBLE, INDEX Server_Render_Speed(Server_Render_Speed),
                        Current_User_ID INT, INDEX Current_User_ID(Current_User_ID),
                        IP_Address	VARCHAR(20), 	INDEX IP_Address(IP_Address)
                    ) ENGINE=MyISAM;
                ";

                        if (dbInfo.TableExists(logTN) == false) {
                            dbInfo.ExecuteSQL(createTableSQL, ref success);
                        }

                        //_____ Parse the page name
                        string pageName = "";
                        {
                            // remove the ?...... page params
                            string[] bits1 = pageURL.Split(new string[] { "?", "&" }, StringSplitOptions.None);
                            string[] bits2 = bits1[0].Split(new string[] { "/", "\\" }, StringSplitOptions.None);
                            pageName = bits2[bits2.Length - 1];
                        }

                        //_____ Build the SQL and run the insert ...
                        StringBuilder sql = new StringBuilder();
                        sql.Append("INSERT INTO " + logTN
                            + " (Application_Name, Session_ID_ASP, Session_ID_Unique, Page_Name, Page_URL, Page_Request_Date, Server_Render_Speed, Current_User_ID, IP_Address ) VALUES (");
                        sql.Append(DataUtilities.Quote(applicationName) + ", ");
                        sql.Append(DataUtilities.Quote(aspSessionID) + ", ");
                        sql.Append(DataUtilities.Quote(uniqueSessionID) + ", ");
                        sql.Append(DataUtilities.Quote(pageName) + ", ");
                        sql.Append(DataUtilities.Quote(pageURL) + ", ");
                        sql.Append(DataUtilities.Quote(DateTimeInformation.FormatDatabaseDate(pageRequestTime, true, true)) + ", ");
                        sql.Append(serverProcessingMS + ", ");
                        sql.Append(currentUserID + ", ");
                        sql.Append(DataUtilities.DatabaseifyString(ipAddress, false));

                        sql.Append(");");

                        int numInserts = dbInfo.ExecuteSQL(sql.ToString(), ref success);

                        if (success == false) {
                            Logger.LogError(201, "Error inserting the PageRequest log into the database - check the detailed log for details.");
                        }
                    }
                }

            } catch (Exception ex) {
                Logger.LogError(202, "Error inserting the PageRequest log into the database: " + ex.ToString());
            } finally {
                if (dbInfo != null) {
                    dbInfo.Disconnect();
                }
            }
        }



        //-----------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>
        ///     15-Jul-15 - Alternative to Captcha ... check that:
        ///         The page has already been requested in the SAME Session
        ///         A reasonable amount of time ago i.e. not immediately and not for ever -
        ///         responseSpeed - 1 is default: 5 seconds is about the fastest people could fill the form in and up to 5 minutes is reasonable to kick em out ...
        ///         2 is faster between 1 seconds and 5 mins
        /// </summary>
        public static bool IsHuman(ConfigurationInfo ci, string uniqueSessionID, string pageName, int responseSpeed) {

            bool isHuman = false;

            DatabaseWrapper dbInfo = null;

            try {

                //_____ Connect to the database ...
                dbInfo = new DatabaseWrapper(ci);
                dbInfo.Connect();

                //_____ Create the table if it does not already exist
                string selectSQL = "SELECT Page_Request_Date FROM " + logTN
                    + " WHERE Session_ID_Unique="+DataUtilities.Quote(uniqueSessionID)
                    + " AND Page_Name=" + DataUtilities.Quote(pageName) + " ORDER BY Page_Request_Date DESC LIMIT 1;";

                string[] vals = dbInfo.ReadLine(selectSQL);

                if (vals == null || vals.Length != 1) {
                    Logger.LogError(111, "Possible Spambot - Session postback but first time session requested this page (PageRequestDate not available when checking IsHuman).");
                } else {

                    DateTime origRequest = DateTimeInformation.NullDate;
                    DateTime.TryParse(vals[0], out origRequest);

                    TimeSpan t = DateTime.Now.Subtract(origRequest);

                    // more than 12 seconds is a Loooong time for a bot.  But still quite fast for a human.
                    if (responseSpeed == 1 && t.TotalSeconds > 5 && t.TotalSeconds < 300) {
                        isHuman = true;
                    } else if (responseSpeed == 2 && t.TotalMilliseconds > 1000 && t.TotalSeconds < 300) {
                        isHuman = true;
                    } else {
                        Logger.LogError(112, "Possible Spambot - The response time in the IsHuman check looks ... inhuman! Page: "+pageName
                            +" and Unique Sesh ID: "+uniqueSessionID+".  The total seconds recorded was: "+t.TotalSeconds);
                    }
                }

            } catch (Exception ex) {
                Logger.LogError(7, "Error checking whether the response is human or not: " + ex.ToString());
            } finally {
                if (dbInfo != null) {
                    dbInfo.Disconnect();
                }
            }

            return isHuman;
        }


        //-----------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>
        ///     3-Mar-2015 - ignore specific IP addresses from the log - currently the only one is to ignore the UptimeRobot ping IP ...
        /// </summary>
        private static List<string> AddressesToIgnore = new List<string> {
            "69.162.124.228" // UptimeRobot
        };

    }  // End of Class
}
